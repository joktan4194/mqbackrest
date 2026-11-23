package pipeline

import (
	"bytes"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"testing"
)

func TestRoundTrip(t *testing.T) {
	key := make([]byte, 32)
	rand.Read(key)
	data := bytes.Repeat([]byte("mqbackrest backs up mysql "), 50_000)
	for _, o := range []Options{{}, {Compress: true, Level: 3}, {Key: key}, {Compress: true, Level: 19, Key: key}} {
		var buf bytes.Buffer
		enc, err := NewEncoder(&buf, o)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := enc.Write(data); err != nil {
			t.Fatal(err)
		}
		if err := enc.Close(); err != nil {
			t.Fatal(err)
		}
		res := enc.Result()
		sum := sha256.Sum256(buf.Bytes())
		if res.SHA256 != hex.EncodeToString(sum[:]) || res.Size != int64(buf.Len()) || res.RawSize != int64(len(data)) {
			t.Fatalf("bad result %+v", res)
		}
		if o.Compress && res.Size >= res.RawSize {
			t.Fatalf("compression ineffective: %d >= %d", res.Size, res.RawSize)
		}
		dec, err := NewDecoder(&buf, o)
		if err != nil {
			t.Fatal(err)
		}
		got, err := io.ReadAll(dec)
		dec.Close()
		if err != nil || !bytes.Equal(got, data) {
			t.Fatalf("decode mismatch (opts %+v): %v", o, err)
		}
	}
}
