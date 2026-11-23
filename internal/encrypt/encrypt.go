// Package encrypt implements mqBackRest's streaming authenticated encryption.
//
// Stream format (all integers big-endian):
//
//	magic   "MQBRENC1"            8 bytes
//	prefix  random nonce prefix   8 bytes
//	chunks  repeated:
//	        header uint32  bit31 = final flag, bits0-30 = ciphertext length
//	        ciphertext     AES-256-GCM(plaintext<=64KiB), nonce = prefix||counter(uint32),
//	                       additional data = {final flag byte}
//
// Every chunk is authenticated, chunk order is bound by the counter, and the
// final flag makes truncation detectable.
package encrypt

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/pbkdf2"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
)

const (
	chunkSize  = 64 * 1024
	finalBit   = 1 << 31
	headerSize = 4
	kdfIters   = 600_000
	keySize    = 32
)

var magic = []byte("MQBRENC1")

var (
	ErrTruncated = errors.New("encrypted stream truncated")
	ErrAuth      = errors.New("encrypted stream failed authentication (wrong key or corrupt data)")
	ErrBadMagic  = errors.New("not a mqbackrest encrypted stream")
)

// KeyEnvelope is the repository master key wrapped by a passphrase-derived key.
// It is stored in stanza.json; the passphrase itself is never stored.
type KeyEnvelope struct {
	KDF        string `json:"kdf"`
	Iterations int    `json:"iterations"`
	Salt       []byte `json:"salt"`
	Nonce      []byte `json:"nonce"`
	Wrapped    []byte `json:"wrapped"`
}

// NewMasterKey creates a random repository key wrapped with the passphrase.
func NewMasterKey(pass string) (*KeyEnvelope, []byte, error) {
	if pass == "" {
		return nil, nil, errors.New("empty cipher passphrase")
	}
	key := make([]byte, keySize)
	salt := make([]byte, 16)
	nonce := make([]byte, 12)
	for _, b := range [][]byte{key, salt, nonce} {
		if _, err := rand.Read(b); err != nil {
			return nil, nil, err
		}
	}
	kek, err := pbkdf2.Key(sha256.New, pass, salt, kdfIters, keySize)
	if err != nil {
		return nil, nil, err
	}
	aead, err := newAEAD(kek)
	if err != nil {
		return nil, nil, err
	}
	env := &KeyEnvelope{
		KDF: "pbkdf2-sha256", Iterations: kdfIters, Salt: salt, Nonce: nonce,
		Wrapped: aead.Seal(nil, nonce, key, []byte("mqbackrest-master-key")),
	}
	return env, key, nil
}

// Unwrap recovers the repository key from the envelope.
func (e *KeyEnvelope) Unwrap(pass string) ([]byte, error) {
	if e.KDF != "pbkdf2-sha256" {
		return nil, fmt.Errorf("unsupported kdf %q", e.KDF)
	}
	kek, err := pbkdf2.Key(sha256.New, pass, e.Salt, e.Iterations, keySize)
	if err != nil {
		return nil, err
	}
	aead, err := newAEAD(kek)
	if err != nil {
		return nil, err
	}
	key, err := aead.Open(nil, e.Nonce, e.Wrapped, []byte("mqbackrest-master-key"))
	if err != nil {
		return nil, errors.New("wrong repo cipher passphrase")
	}
	return key, nil
}

func newAEAD(key []byte) (cipher.AEAD, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	return cipher.NewGCM(block)
}

func nonceFor(prefix []byte, counter uint32) []byte {
	n := make([]byte, 12)
	copy(n, prefix)
	binary.BigEndian.PutUint32(n[8:], counter)
	return n
}

type writer struct {
	w       io.Writer
	aead    cipher.AEAD
	prefix  []byte
	counter uint32
	buf     []byte
	closed  bool
}

// NewWriter returns a WriteCloser that encrypts to w. Close must be called.
func NewWriter(w io.Writer, key []byte) (io.WriteCloser, error) {
	aead, err := newAEAD(key)
	if err != nil {
		return nil, err
	}
	prefix := make([]byte, 8)
	if _, err := rand.Read(prefix); err != nil {
		return nil, err
	}
	if _, err := w.Write(append(append([]byte{}, magic...), prefix...)); err != nil {
		return nil, err
	}
	return &writer{w: w, aead: aead, prefix: prefix, buf: make([]byte, 0, chunkSize)}, nil
}

func (e *writer) Write(p []byte) (int, error) {
	if e.closed {
		return 0, errors.New("write to closed encrypt writer")
	}
	n := 0
	for len(p) > 0 {
		take := min(chunkSize-len(e.buf), len(p))
		e.buf = append(e.buf, p[:take]...)
		p, n = p[take:], n+take
		if len(e.buf) == chunkSize {
			if err := e.flush(false); err != nil {
				return n, err
			}
		}
	}
	return n, nil
}

func (e *writer) flush(final bool) error {
	var flag byte
	if final {
		flag = 1
	}
	ct := e.aead.Seal(nil, nonceFor(e.prefix, e.counter), e.buf, []byte{flag})
	hdr := uint32(len(ct))
	if final {
		hdr |= finalBit
	}
	var h [headerSize]byte
	binary.BigEndian.PutUint32(h[:], hdr)
	if _, err := e.w.Write(h[:]); err != nil {
		return err
	}
	if _, err := e.w.Write(ct); err != nil {
		return err
	}
	e.counter++
	if e.counter == 0 {
		return errors.New("encrypt stream too long")
	}
	e.buf = e.buf[:0]
	return nil
}

func (e *writer) Close() error {
	if e.closed {
		return nil
	}
	e.closed = true
	return e.flush(true)
}

type reader struct {
	r       io.Reader
	aead    cipher.AEAD
	prefix  []byte
	counter uint32
	plain   []byte
	done    bool
	ct      []byte
}

// NewReader returns a Reader that decrypts and authenticates r.
func NewReader(r io.Reader, key []byte) (io.Reader, error) {
	aead, err := newAEAD(key)
	if err != nil {
		return nil, err
	}
	head := make([]byte, len(magic)+8)
	if _, err := io.ReadFull(r, head); err != nil {
		return nil, ErrBadMagic
	}
	if string(head[:len(magic)]) != string(magic) {
		return nil, ErrBadMagic
	}
	return &reader{r: r, aead: aead, prefix: head[len(magic):], ct: make([]byte, chunkSize+aead.Overhead())}, nil
}

func (d *reader) Read(p []byte) (int, error) {
	for len(d.plain) == 0 {
		if d.done {
			return 0, io.EOF
		}
		var h [headerSize]byte
		if _, err := io.ReadFull(d.r, h[:]); err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				return 0, ErrTruncated
			}
			return 0, err
		}
		hdr := binary.BigEndian.Uint32(h[:])
		final := hdr&finalBit != 0
		n := int(hdr &^ finalBit)
		if n > len(d.ct) || n < d.aead.Overhead() {
			return 0, ErrAuth
		}
		if _, err := io.ReadFull(d.r, d.ct[:n]); err != nil {
			return 0, ErrTruncated
		}
		var flag byte
		if final {
			flag = 1
		}
		pt, err := d.aead.Open(d.ct[:0:0], nonceFor(d.prefix, d.counter), d.ct[:n], []byte{flag})
		if err != nil {
			return 0, ErrAuth
		}
		d.counter++
		d.plain = pt
		d.done = final
	}
	n := copy(p, d.plain)
	d.plain = d.plain[n:]
	return n, nil
}
