// Package pipeline chains compression, encryption and hashing for stored files.
//
// Write path: plaintext -> zstd -> encrypt -> sha256/size -> storage
// Read path:  storage -> decrypt -> zstd -> plaintext
package pipeline

import (
	"crypto/sha256"
	"encoding/hex"
	"hash"
	"io"

	"github.com/klauspost/compress/zstd"

	"github.com/joktan4194/mqbackrest/internal/encrypt"
)

// Options selects the transforms applied to a file.
type Options struct {
	Compress bool
	Level    int    // zstd level 1-22
	Key      []byte // nil disables encryption
}

// Result describes an encoded file. SHA256 and Size cover the stored bytes,
// so integrity can be verified without the encryption key.
type Result struct {
	RawSize int64  `json:"raw_size"`
	Size    int64  `json:"size"`
	SHA256  string `json:"sha256"`
}

type counter struct {
	w io.Writer
	n int64
	h hash.Hash
}

func (c *counter) Write(p []byte) (int, error) {
	n, err := c.w.Write(p)
	c.n += int64(n)
	if c.h != nil {
		c.h.Write(p[:n])
	}
	return n, err
}

// Encoder is an io.WriteCloser producing an encoded stream.
type Encoder struct {
	raw    *counter
	stored *counter
	layers []io.Closer // closed innermost-first
}

// NewEncoder wraps dst. Call Close to flush; then Result is valid.
func NewEncoder(dst io.Writer, o Options) (*Encoder, error) {
	stored := &counter{w: dst, h: sha256.New()}
	e := &Encoder{stored: stored}
	var w io.Writer = stored
	if o.Key != nil {
		ew, err := encrypt.NewWriter(w, o.Key)
		if err != nil {
			return nil, err
		}
		e.layers = append([]io.Closer{ew}, e.layers...)
		w = ew
	}
	if o.Compress {
		zw, err := zstd.NewWriter(w, zstd.WithEncoderLevel(zstd.EncoderLevelFromZstd(o.Level)))
		if err != nil {
			return nil, err
		}
		e.layers = append([]io.Closer{zw}, e.layers...)
		w = zw
	}
	e.raw = &counter{w: w}
	return e, nil
}

func (e *Encoder) Write(p []byte) (int, error) { return e.raw.Write(p) }

// Close flushes every layer, outermost (compression) first.
func (e *Encoder) Close() error {
	for _, l := range e.layers {
		if err := l.Close(); err != nil {
			return err
		}
	}
	return nil
}

// Result returns sizes and the stored-bytes checksum. Valid after Close.
func (e *Encoder) Result() Result {
	return Result{RawSize: e.raw.n, Size: e.stored.n, SHA256: hex.EncodeToString(e.stored.h.Sum(nil))}
}

// NewDecoder returns the plaintext of an encoded stream.
func NewDecoder(src io.Reader, o Options) (io.ReadCloser, error) {
	r := src
	if o.Key != nil {
		dr, err := encrypt.NewReader(r, o.Key)
		if err != nil {
			return nil, err
		}
		r = dr
	}
	if !o.Compress {
		return io.NopCloser(r), nil
	}
	zr, err := zstd.NewReader(r, zstd.WithDecoderConcurrency(1))
	if err != nil {
		return nil, err
	}
	return zr.IOReadCloser(), nil
}
