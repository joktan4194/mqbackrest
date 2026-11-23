package repo

import (
	"context"
	"fmt"
	"io"
	"sort"
	"strings"
	"time"

	"github.com/joktan4194/mqbackrest/internal/pipeline"
)

// Backup types.
const (
	TypeFull = "full"
	TypeIncr = "incr"
)

// BinlogPos is the binary log coordinate consistent with a backup.
type BinlogPos struct {
	File     string `json:"file"`
	Position uint64 `json:"position"`
	GTIDSet  string `json:"gtid_set,omitempty"`
}

// Part is one stored chunk of the xbstream.
type Part struct {
	Name string `json:"name"` // relative to the backup dir
	pipeline.Result
}

// Manifest fully describes a completed backup.
type Manifest struct {
	Label             string     `json:"label"`
	Type              string     `json:"type"`
	Prior             string     `json:"prior,omitempty"`
	StartTime         time.Time  `json:"start_time"`
	StopTime          time.Time  `json:"stop_time"`
	FromLSN           uint64     `json:"from_lsn"`
	ToLSN             uint64     `json:"to_lsn"`
	LastLSN           uint64     `json:"last_lsn"`
	Binlog            *BinlogPos `json:"binlog,omitempty"`
	ServerVersion     string     `json:"server_version,omitempty"`
	ToolVersion       string     `json:"tool_version,omitempty"`
	MQBackRestVersion string     `json:"mqbackrest_version"`
	Compress          string     `json:"compress"`
	Encrypted         bool       `json:"encrypted"`
	RawSize           int64      `json:"raw_size"`
	Size              int64      `json:"size"`
	Parts             []Part     `json:"parts"`
	Checkpoints       string     `json:"xtrabackup_checkpoints,omitempty"`
	Info              string     `json:"xtrabackup_info,omitempty"`
}

// Root returns the label of the full backup this backup depends on.
func (m *Manifest) Root() string { return RootLabel(m.Label) }

// RootLabel returns the full-backup label at the base of label's chain.
func RootLabel(label string) string {
	if i := strings.IndexByte(label, '_'); i >= 0 {
		return label[:i]
	}
	return label
}

// NewLabel builds a sortable backup label. Full: 20261123-101500F.
// Incremental: 20261123-101500F_20261123-121500I.
func NewLabel(t time.Time, typ string, prior *Manifest) string {
	ts := t.UTC().Format("20060102-150405")
	if typ == TypeFull || prior == nil {
		return ts + "F"
	}
	return prior.Root() + "_" + ts + "I"
}

func (r *Repo) backupDir(label string) string { return r.prefix() + "backup/" + label + "/" }

func (r *Repo) manifestKey(label string) string {
	return r.backupDir(label) + "manifest.json" + r.metaExt()
}

// PartName returns the stored name of part idx, relative to the backup dir.
func (r *Repo) PartName(idx int) string { return fmt.Sprintf("data/%06d.xbs%s", idx, r.dataExt()) }

// PutPart stores one part of a backup.
func (r *Repo) PutPart(ctx context.Context, label string, idx int, src io.Reader) (Part, error) {
	name := r.PartName(idx)
	res, err := r.PutObject(ctx, r.backupDir(label)+name, src)
	return Part{Name: name, Result: res}, err
}

// GetPart opens a part for reading; Close verifies its checksum.
func (r *Repo) GetPart(ctx context.Context, m *Manifest, p Part) (io.ReadCloser, error) {
	res := p.Result
	return r.GetObject(ctx, r.backupDir(m.Label)+p.Name, &res)
}

// HashPart checksums a part's stored bytes.
func (r *Repo) HashPart(ctx context.Context, m *Manifest, p Part) (int64, string, error) {
	return r.HashObject(ctx, r.backupDir(m.Label)+p.Name)
}

// SaveManifest writes the manifest, marking the backup complete.
func (r *Repo) SaveManifest(ctx context.Context, m *Manifest) error {
	return r.putJSON(ctx, r.manifestKey(m.Label), m)
}

// BackupSet is the result of scanning the backup directory.
type BackupSet struct {
	Backups []*Manifest // complete, sorted oldest first
	Orphans []string    // labels with data but no manifest (failed or in-progress)
}

// ListBackups loads every manifest in the stanza.
func (r *Repo) ListBackups(ctx context.Context) (*BackupSet, error) {
	objs, err := r.St.List(ctx, r.prefix()+"backup/")
	if err != nil {
		return nil, err
	}
	labels := map[string]bool{}
	for _, o := range objs {
		rest := strings.TrimPrefix(o.Key, r.prefix()+"backup/")
		label, file, ok := strings.Cut(rest, "/")
		if !ok {
			continue
		}
		if file == "manifest.json"+r.metaExt() {
			labels[label] = true
		} else if !labels[label] {
			labels[label] = false
		}
	}
	set := &BackupSet{}
	for label, complete := range labels {
		if !complete {
			set.Orphans = append(set.Orphans, label)
			continue
		}
		var m Manifest
		if err := r.getJSON(ctx, r.manifestKey(label), &m); err != nil {
			return nil, fmt.Errorf("load manifest %s: %w", label, err)
		}
		set.Backups = append(set.Backups, &m)
	}
	sort.Slice(set.Backups, func(i, j int) bool { return set.Backups[i].StartTime.Before(set.Backups[j].StartTime) })
	sort.Strings(set.Orphans)
	return set, nil
}

// Latest returns the most recent backup, or nil.
func (s *BackupSet) Latest() *Manifest {
	if len(s.Backups) == 0 {
		return nil
	}
	return s.Backups[len(s.Backups)-1]
}

// Find returns the backup with label, or nil.
func (s *BackupSet) Find(label string) *Manifest {
	for _, b := range s.Backups {
		if b.Label == label {
			return b
		}
	}
	return nil
}

// Chain returns the backups needed to restore label, full backup first.
func (s *BackupSet) Chain(label string) ([]*Manifest, error) {
	var chain []*Manifest
	for cur := label; cur != ""; {
		m := s.Find(cur)
		if m == nil {
			return nil, fmt.Errorf("backup %s (needed by %s) not found in repository", cur, label)
		}
		chain = append([]*Manifest{m}, chain...)
		if len(chain) > 10_000 {
			return nil, fmt.Errorf("backup chain for %s is cyclic", label)
		}
		cur = m.Prior
	}
	if chain[0].Type != TypeFull {
		return nil, fmt.Errorf("backup chain for %s does not start with a full backup", label)
	}
	return chain, nil
}

// DeleteBackup removes every object belonging to label. The manifest goes
// first so a half-deleted backup is never mistaken for a complete one.
func (r *Repo) DeleteBackup(ctx context.Context, label string) error {
	if err := r.St.Delete(ctx, r.manifestKey(label)); err != nil {
		return err
	}
	objs, err := r.St.List(ctx, r.backupDir(label))
	if err != nil {
		return err
	}
	for _, o := range objs {
		if err := r.St.Delete(ctx, o.Key); err != nil {
			return err
		}
	}
	return nil
}
