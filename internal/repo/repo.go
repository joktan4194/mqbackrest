// Package repo implements the mqBackRest repository layout on top of a Storage.
//
//	<stanza>/stanza.json                         stanza metadata + wrapped key
//	<stanza>/backup/<label>/manifest.json[.enc]  written last: its presence marks a complete backup
//	<stanza>/backup/<label>/data/NNNNNN.xbs[.zst][.enc]
//	<stanza>/binlog/<name>[.zst][.enc]           archived binary log
//	<stanza>/binlog/<name>.json[.enc]            binlog sidecar (checksum, sizes)
package repo

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"hash"
	"io"
	"time"

	"github.com/joktan4194/mqbackrest/internal/config"
	"github.com/joktan4194/mqbackrest/internal/encrypt"
	"github.com/joktan4194/mqbackrest/internal/pipeline"
	"github.com/joktan4194/mqbackrest/internal/storage"
	"github.com/joktan4194/mqbackrest/internal/version"
)

// FormatVersion is bumped on incompatible repository layout changes.
const FormatVersion = 1

// ErrNoStanza means stanza-create has not been run.
var ErrNoStanza = errors.New("stanza does not exist (run `mqbackrest stanza-create`)")

// Stanza is the immutable per-cluster repository metadata.
type Stanza struct {
	Name      string               `json:"name"`
	Format    int                  `json:"format"`
	Created   time.Time            `json:"created"`
	CreatedBy string               `json:"created_by"`
	Compress  string               `json:"compress"`
	Cipher    *encrypt.KeyEnvelope `json:"cipher,omitempty"`
}

// Repo is an opened stanza.
type Repo struct {
	St     storage.Storage
	Stanza *Stanza
	opts   pipeline.Options
}

func stanzaKey(name string) string { return name + "/stanza.json" }

// Create initialises a new stanza. It refuses to overwrite an existing one.
func Create(ctx context.Context, st storage.Storage, cfg *config.Config) (*Stanza, error) {
	if b, ok := st.(interface {
		EnsureBucket(context.Context, string) error
	}); ok {
		if err := b.EnsureBucket(ctx, cfg.Repo.S3.Region); err != nil {
			return nil, fmt.Errorf("ensure bucket: %w", err)
		}
	}
	exists, err := st.Exists(ctx, stanzaKey(cfg.Stanza))
	if err != nil {
		return nil, err
	}
	if exists {
		return nil, fmt.Errorf("stanza %q already exists in %s", cfg.Stanza, st)
	}
	s := &Stanza{
		Name: cfg.Stanza, Format: FormatVersion, Created: time.Now().UTC(),
		CreatedBy: "mqbackrest " + version.Version, Compress: cfg.Compress.Type,
	}
	if cfg.Repo.CipherPass != "" {
		env, _, err := encrypt.NewMasterKey(cfg.Repo.CipherPass)
		if err != nil {
			return nil, err
		}
		s.Cipher = env
	}
	b, _ := json.MarshalIndent(s, "", "  ")
	return s, st.Put(ctx, stanzaKey(cfg.Stanza), bytes.NewReader(b))
}

// Open loads the stanza and unlocks the repository key if encrypted.
func Open(ctx context.Context, st storage.Storage, cfg *config.Config) (*Repo, error) {
	rc, err := st.Get(ctx, stanzaKey(cfg.Stanza))
	if errors.Is(err, storage.ErrNotFound) {
		return nil, ErrNoStanza
	}
	if err != nil {
		return nil, err
	}
	defer rc.Close()
	var s Stanza
	if err := json.NewDecoder(rc).Decode(&s); err != nil {
		return nil, fmt.Errorf("decode stanza.json: %w", err)
	}
	if s.Format > FormatVersion {
		return nil, fmt.Errorf("repository format %d is newer than this mqbackrest supports (%d); upgrade mqbackrest", s.Format, FormatVersion)
	}
	r := &Repo{St: st, Stanza: &s, opts: pipeline.Options{Compress: s.Compress == "zstd", Level: cfg.Compress.Level}}
	switch {
	case s.Cipher != nil && cfg.Repo.CipherPass == "":
		return nil, errors.New("repository is encrypted: set repo.cipher_pass")
	case s.Cipher == nil && cfg.Repo.CipherPass != "":
		return nil, errors.New("repo.cipher_pass is set but the stanza was created unencrypted")
	case s.Cipher != nil:
		key, err := s.Cipher.Unwrap(cfg.Repo.CipherPass)
		if err != nil {
			return nil, err
		}
		r.opts.Key = key
	}
	return r, nil
}

// Encrypted reports whether files are encrypted.
func (r *Repo) Encrypted() bool { return r.opts.Key != nil }

func (r *Repo) dataExt() string {
	ext := ""
	if r.opts.Compress {
		ext += ".zst"
	}
	return ext + r.metaExt()
}

func (r *Repo) metaExt() string {
	if r.opts.Key != nil {
		return ".enc"
	}
	return ""
}

func (r *Repo) prefix() string { return r.Stanza.Name + "/" }

// PutObject encodes src (compress+encrypt) into key.
func (r *Repo) PutObject(ctx context.Context, key string, src io.Reader) (pipeline.Result, error) {
	return r.put(ctx, key, src, r.opts)
}

func (r *Repo) put(ctx context.Context, key string, src io.Reader, o pipeline.Options) (pipeline.Result, error) {
	pr, pw := io.Pipe()
	resCh := make(chan pipeline.Result, 1)
	errCh := make(chan error, 1)
	go func() {
		enc, err := pipeline.NewEncoder(pw, o)
		if err == nil {
			if _, err = io.Copy(enc, src); err == nil {
				err = enc.Close()
			}
		}
		if err == nil {
			resCh <- enc.Result()
		}
		pw.CloseWithError(err)
		errCh <- err
	}()
	putErr := r.St.Put(ctx, key, pr)
	pr.CloseWithError(errors.New("storage put finished"))
	encErr := <-errCh
	if putErr != nil {
		return pipeline.Result{}, fmt.Errorf("put %s: %w", key, putErr)
	}
	if encErr != nil {
		return pipeline.Result{}, fmt.Errorf("encode %s: %w", key, encErr)
	}
	return <-resCh, nil
}

// GetObject returns the decoded plaintext of key. If expect is non-nil the
// stored bytes are checksummed and Close reports any mismatch.
func (r *Repo) GetObject(ctx context.Context, key string, expect *pipeline.Result) (io.ReadCloser, error) {
	return r.get(ctx, key, expect, r.opts)
}

func (r *Repo) get(ctx context.Context, key string, expect *pipeline.Result, o pipeline.Options) (io.ReadCloser, error) {
	raw, err := r.St.Get(ctx, key)
	if err != nil {
		return nil, err
	}
	hr := &hashReader{r: raw, h: sha256.New()}
	dec, err := pipeline.NewDecoder(hr, o)
	if err != nil {
		raw.Close()
		return nil, fmt.Errorf("decode %s: %w", key, err)
	}
	return &verifyReader{ReadCloser: dec, raw: raw, hr: hr, key: key, expect: expect}, nil
}

// HashObject checksums the stored bytes of key without decoding.
func (r *Repo) HashObject(ctx context.Context, key string) (int64, string, error) {
	rc, err := r.St.Get(ctx, key)
	if err != nil {
		return 0, "", err
	}
	defer rc.Close()
	h := sha256.New()
	n, err := io.Copy(h, rc)
	return n, hex.EncodeToString(h.Sum(nil)), err
}

func (r *Repo) putJSON(ctx context.Context, key string, v any) error {
	b, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}
	_, err = r.put(ctx, key, bytes.NewReader(b), pipeline.Options{Key: r.opts.Key})
	return err
}

func (r *Repo) getJSON(ctx context.Context, key string, v any) error {
	rc, err := r.get(ctx, key, nil, pipeline.Options{Key: r.opts.Key})
	if err != nil {
		return err
	}
	defer rc.Close()
	return json.NewDecoder(rc).Decode(v)
}

type hashReader struct {
	r io.Reader
	h hash.Hash
	n int64
}

func (h *hashReader) Read(p []byte) (int, error) {
	n, err := h.r.Read(p)
	h.h.Write(p[:n])
	h.n += int64(n)
	return n, err
}

type verifyReader struct {
	io.ReadCloser
	raw    io.Closer
	hr     *hashReader
	key    string
	expect *pipeline.Result
}

func (v *verifyReader) Close() error {
	var err error
	if v.expect != nil {
		_, _ = io.Copy(io.Discard, v.hr) // consume any trailing bytes so the hash covers the whole object
		sum := hex.EncodeToString(v.hr.h.Sum(nil))
		if sum != v.expect.SHA256 || v.hr.n != v.expect.Size {
			err = fmt.Errorf("%s: checksum mismatch (stored %d bytes sha256 %s, manifest %d bytes sha256 %s)",
				v.key, v.hr.n, sum, v.expect.Size, v.expect.SHA256)
		}
	}
	v.ReadCloser.Close()
	if cerr := v.raw.Close(); err == nil {
		err = cerr
	}
	return err
}
