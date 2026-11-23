package repo

import (
	"context"
	"fmt"
	"io"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/joktan4194/mqbackrest/internal/pipeline"
)

// BinlogName is a parsed binary log file name such as "binlog.000042".
type BinlogName struct {
	Base  string
	Seq   uint64
	Width int
}

// ParseBinlogName splits "binlog.000042" into base and sequence.
func ParseBinlogName(name string) (BinlogName, bool) {
	i := strings.LastIndexByte(name, '.')
	if i <= 0 || i == len(name)-1 {
		return BinlogName{}, false
	}
	digits := name[i+1:]
	seq, err := strconv.ParseUint(digits, 10, 64)
	if err != nil {
		return BinlogName{}, false
	}
	return BinlogName{Base: name[:i], Seq: seq, Width: len(digits)}, true
}

func (b BinlogName) String() string { return fmt.Sprintf("%s.%0*d", b.Base, b.Width, b.Seq) }

// Next returns the following binlog name.
func (b BinlogName) Next() BinlogName { b.Seq++; return b }

// BinlogMeta is the sidecar stored next to each archived binlog.
type BinlogMeta struct {
	Name       string    `json:"name"`
	ArchivedAt time.Time `json:"archived_at"`
	pipeline.Result
}

func (r *Repo) binlogKey(name string) string { return r.prefix() + "binlog/" + name + r.dataExt() }
func (r *Repo) binlogMetaKey(name string) string {
	return r.prefix() + "binlog/" + name + ".json" + r.metaExt()
}

// PutBinlog archives one complete binary log. Re-archiving is idempotent.
func (r *Repo) PutBinlog(ctx context.Context, name string, src io.Reader) (*BinlogMeta, error) {
	res, err := r.PutObject(ctx, r.binlogKey(name), src)
	if err != nil {
		return nil, err
	}
	meta := &BinlogMeta{Name: name, ArchivedAt: time.Now().UTC(), Result: res}
	return meta, r.putJSON(ctx, r.binlogMetaKey(name), meta)
}

// GetBinlog opens an archived binlog; Close verifies its checksum.
func (r *Repo) GetBinlog(ctx context.Context, m *BinlogMeta) (io.ReadCloser, error) {
	res := m.Result
	return r.GetObject(ctx, r.binlogKey(m.Name), &res)
}

// HashBinlog checksums an archived binlog's stored bytes.
func (r *Repo) HashBinlog(ctx context.Context, m *BinlogMeta) (int64, string, error) {
	return r.HashObject(ctx, r.binlogKey(m.Name))
}

// ListBinlogs returns archived binlogs sorted by sequence.
func (r *Repo) ListBinlogs(ctx context.Context) ([]*BinlogMeta, error) {
	objs, err := r.St.List(ctx, r.prefix()+"binlog/")
	if err != nil {
		return nil, err
	}
	suffix := ".json" + r.metaExt()
	var out []*BinlogMeta
	for _, o := range objs {
		name := strings.TrimPrefix(o.Key, r.prefix()+"binlog/")
		if !strings.HasSuffix(name, suffix) {
			continue
		}
		var m BinlogMeta
		if err := r.getJSON(ctx, o.Key, &m); err != nil {
			return nil, fmt.Errorf("load binlog meta %s: %w", name, err)
		}
		out = append(out, &m)
	}
	SortBinlogs(out)
	return out, nil
}

// SortBinlogs orders by base name then sequence.
func SortBinlogs(bs []*BinlogMeta) {
	sort.Slice(bs, func(i, j int) bool {
		a, _ := ParseBinlogName(bs[i].Name)
		b, _ := ParseBinlogName(bs[j].Name)
		if a.Base != b.Base {
			return a.Base < b.Base
		}
		return a.Seq < b.Seq
	})
}

// BinlogGaps reports missing sequence numbers in an archive.
func BinlogGaps(bs []*BinlogMeta) []string {
	var gaps []string
	for i := 1; i < len(bs); i++ {
		prev, ok1 := ParseBinlogName(bs[i-1].Name)
		cur, ok2 := ParseBinlogName(bs[i].Name)
		if !ok1 || !ok2 || prev.Base != cur.Base {
			continue
		}
		for s := prev.Seq + 1; s < cur.Seq; s++ {
			gaps = append(gaps, BinlogName{Base: cur.Base, Seq: s, Width: cur.Width}.String())
		}
	}
	return gaps
}

// DeleteBinlog removes an archived binlog and its sidecar.
func (r *Repo) DeleteBinlog(ctx context.Context, name string) error {
	if err := r.St.Delete(ctx, r.binlogMetaKey(name)); err != nil {
		return err
	}
	return r.St.Delete(ctx, r.binlogKey(name))
}
