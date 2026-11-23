package encrypt

import (
	"bytes"
	"crypto/rand"
	"errors"
	"io"
	"testing"
)

func roundTrip(t *testing.T, key, data []byte) []byte {
	t.Helper()
	var buf bytes.Buffer
	w, err := NewWriter(&buf, key)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.Write(data); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

func TestRoundTripSizes(t *testing.T) {
	key := make([]byte, 32)
	rand.Read(key)
	for _, n := range []int{0, 1, chunkSize - 1, chunkSize, chunkSize + 1, 3*chunkSize + 17} {
		data := make([]byte, n)
		rand.Read(data)
		enc := roundTrip(t, key, data)
		r, err := NewReader(bytes.NewReader(enc), key)
		if err != nil {
			t.Fatal(err)
		}
		got, err := io.ReadAll(r)
		if err != nil {
			t.Fatalf("n=%d: %v", n, err)
		}
		if !bytes.Equal(got, data) {
			t.Fatalf("n=%d: mismatch", n)
		}
	}
}

func TestTamperAndTruncation(t *testing.T) {
	key := make([]byte, 32)
	rand.Read(key)
	data := make([]byte, 2*chunkSize+5)
	enc := roundTrip(t, key, data)

	bad := append([]byte{}, enc...)
	bad[len(bad)/2] ^= 0xff
	r, _ := NewReader(bytes.NewReader(bad), key)
	if _, err := io.ReadAll(r); !errors.Is(err, ErrAuth) {
		t.Fatalf("tamper: want ErrAuth, got %v", err)
	}

	// Drop the final chunk entirely: must be reported as truncation.
	cut := enc[:16+4+chunkSize+16]
	r, _ = NewReader(bytes.NewReader(cut), key)
	if _, err := io.ReadAll(r); !errors.Is(err, ErrTruncated) {
		t.Fatalf("truncate: want ErrTruncated, got %v", err)
	}

	wrong := make([]byte, 32)
	r, _ = NewReader(bytes.NewReader(enc), wrong)
	if _, err := io.ReadAll(r); !errors.Is(err, ErrAuth) {
		t.Fatalf("wrong key: want ErrAuth, got %v", err)
	}
}

func TestMasterKey(t *testing.T) {
	env, key, err := NewMasterKey("correct horse")
	if err != nil {
		t.Fatal(err)
	}
	got, err := env.Unwrap("correct horse")
	if err != nil || !bytes.Equal(got, key) {
		t.Fatalf("unwrap failed: %v", err)
	}
	if _, err := env.Unwrap("nope"); err == nil {
		t.Fatal("expected wrong passphrase error")
	}
}
