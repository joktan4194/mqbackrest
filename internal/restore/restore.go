// Package restore orchestrates `mqbackrest restore` and `mqbackrest replay`.
package restore

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"time"

	"github.com/joktan4194/mqbackrest/internal/repo"
	"github.com/joktan4194/mqbackrest/internal/xtrabackup"
)

// TimeLayout is the accepted --target-time format (matches mysqlbinlog --stop-datetime).
const TimeLayout = "2006-01-02 15:04:05"

// Options for a restore.
type Options struct {
	Set            string // label or "latest"
	TargetDir      string // MySQL datadir to restore into
	WorkDir        string // staging dir for extract/prepare
	TargetTime     string // optional PITR target, TimeLayout, server-local time
	PITRDir        string // where binlogs + replay plan are written
	DeleteExisting bool
	KeepWork       bool
}

// ReplayPlan is written to <PITRDir>/replay.json for `mqbackrest replay`.
type ReplayPlan struct {
	Backup        string    `json:"backup"`
	StartFile     string    `json:"start_file"`
	StartPosition uint64    `json:"start_position"`
	StopDatetime  string    `json:"stop_datetime,omitempty"`
	Files         []string  `json:"files"`
	CreatedAt     time.Time `json:"created_at"`
}

// Run restores a backup chain into TargetDir.
func Run(ctx context.Context, r *repo.Repo, xb *xtrabackup.Runner, o Options, log *slog.Logger) error {
	set, err := r.ListBackups(ctx)
	if err != nil {
		return err
	}
	target, err := pickBackup(set, o.Set, o.TargetTime)
	if err != nil {
		return err
	}
	chain, err := set.Chain(target.Label)
	if err != nil {
		return err
	}
	if err := prepareTargetDir(o.TargetDir, o.DeleteExisting); err != nil {
		return err
	}
	work := o.WorkDir
	if work == "" {
		work = filepath.Join(filepath.Dir(filepath.Clean(o.TargetDir)), ".mqbackrest-restore-"+target.Label)
	}
	if err := os.MkdirAll(work, 0o750); err != nil {
		return err
	}
	if !o.KeepWork {
		defer os.RemoveAll(work)
	}
	log.Info("restore start", "backup", target.Label, "chain", len(chain), "work_dir", work)

	dirs := make([]string, len(chain))
	for i, m := range chain {
		dirs[i] = filepath.Join(work, fmt.Sprintf("%02d-%s", i, m.Label))
		if err := os.MkdirAll(dirs[i], 0o750); err != nil {
			return err
		}
		log.Info("downloading", "backup", m.Label, "type", m.Type, "parts", len(m.Parts))
		if err := extract(ctx, r, xb, m, dirs[i]); err != nil {
			return fmt.Errorf("extract %s: %w", m.Label, err)
		}
	}

	log.Info("preparing", "base", chain[0].Label)
	if err := xb.Prepare(ctx, dirs[0], "", len(chain) > 1); err != nil {
		return err
	}
	for i := 1; i < len(chain); i++ {
		last := i == len(chain)-1
		log.Info("applying incremental", "backup", chain[i].Label)
		if err := xb.Prepare(ctx, dirs[0], dirs[i], !last); err != nil {
			return err
		}
	}

	var plan *ReplayPlan
	if o.TargetTime != "" {
		pitr := o.PITRDir
		if pitr == "" {
			pitr = filepath.Join(filepath.Dir(filepath.Clean(o.TargetDir)), "mqbackrest-pitr-"+target.Label)
		}
		if plan, err = fetchBinlogs(ctx, r, target, pitr, o.TargetTime, log); err != nil {
			return err
		}
		o.PITRDir = pitr
	}

	log.Info("moving files into place", "datadir", o.TargetDir)
	if err := xb.MoveBack(ctx, dirs[0], o.TargetDir); err != nil {
		return err
	}
	log.Info("restore complete", "backup", target.Label, "datadir", o.TargetDir)
	fmt.Printf("\nRestored %s into %s.\nNext steps:\n  1. chown -R mysql:mysql %s\n  2. start mysqld\n", target.Label, o.TargetDir, o.TargetDir)
	if plan != nil {
		fmt.Printf("  3. mqbackrest replay --pitr-dir %s   # rolls forward to %s\n", o.PITRDir, plan.StopDatetime)
	}
	return nil
}

func pickBackup(set *repo.BackupSet, label, targetTime string) (*repo.Manifest, error) {
	if len(set.Backups) == 0 {
		return nil, errors.New("repository has no backups")
	}
	if label != "" && label != "latest" {
		m := set.Find(label)
		if m == nil {
			return nil, fmt.Errorf("backup %s not found", label)
		}
		if targetTime != "" {
			t, err := time.ParseInLocation(TimeLayout, targetTime, time.Local)
			if err != nil {
				return nil, fmt.Errorf("--target-time must look like %q: %w", TimeLayout, err)
			}
			if m.StopTime.After(t) {
				return nil, fmt.Errorf("backup %s finished at %s, after target time %s", m.Label, m.StopTime.Local().Format(TimeLayout), targetTime)
			}
		}
		return m, nil
	}
	if targetTime == "" {
		return set.Latest(), nil
	}
	// PITR: newest backup that finished before the target and has a binlog position.
	t, err := time.ParseInLocation(TimeLayout, targetTime, time.Local)
	if err != nil {
		return nil, fmt.Errorf("--target-time must look like %q: %w", TimeLayout, err)
	}
	for i := len(set.Backups) - 1; i >= 0; i-- {
		b := set.Backups[i]
		if !b.StopTime.After(t) && b.Binlog != nil {
			return b, nil
		}
	}
	return nil, fmt.Errorf("no backup with a binlog position finished before %s", targetTime)
}

func prepareTargetDir(dir string, deleteExisting bool) error {
	if dir == "" {
		return errors.New("--target-dir is required")
	}
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return err
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		return err
	}
	if len(entries) == 0 {
		return nil
	}
	if !deleteExisting {
		return fmt.Errorf("target dir %s is not empty (stop mysqld and pass --delete-existing to wipe it)", dir)
	}
	if _, err := os.Stat(filepath.Join(dir, "mysql.sock.lock")); err == nil {
		return fmt.Errorf("%s looks like a running server's datadir (mysql.sock.lock present); stop mysqld first", dir)
	}
	for _, e := range entries {
		if err := os.RemoveAll(filepath.Join(dir, e.Name())); err != nil {
			return err
		}
	}
	return nil
}

// extract downloads parts in order, verifying each, and pipes them into xbstream.
func extract(ctx context.Context, r *repo.Repo, xb *xtrabackup.Runner, m *repo.Manifest, dir string) error {
	pr, pw := io.Pipe()
	done := make(chan error, 1)
	go func() {
		err := xb.Extract(ctx, dir, pr)
		pr.CloseWithError(err)
		done <- err
	}()
	copyErr := func() error {
		for _, p := range m.Parts {
			rc, err := r.GetPart(ctx, m, p)
			if err != nil {
				return err
			}
			_, err = io.Copy(pw, rc)
			if cerr := rc.Close(); err == nil {
				err = cerr
			}
			if err != nil {
				return fmt.Errorf("part %s: %w", p.Name, err)
			}
		}
		return nil
	}()
	pw.CloseWithError(copyErr)
	extractErr := <-done
	if copyErr != nil {
		return copyErr
	}
	return extractErr
}

func fetchBinlogs(ctx context.Context, r *repo.Repo, b *repo.Manifest, dir, targetTime string, log *slog.Logger) (*ReplayPlan, error) {
	if b.Binlog == nil {
		return nil, fmt.Errorf("backup %s has no binlog position; cannot do point-in-time recovery", b.Label)
	}
	start, ok := repo.ParseBinlogName(b.Binlog.File)
	if !ok {
		return nil, fmt.Errorf("cannot parse binlog name %q", b.Binlog.File)
	}
	all, err := r.ListBinlogs(ctx)
	if err != nil {
		return nil, err
	}
	var need []*repo.BinlogMeta
	for _, bl := range all {
		n, ok := repo.ParseBinlogName(bl.Name)
		if ok && n.Base == start.Base && n.Seq >= start.Seq {
			need = append(need, bl)
		}
	}
	if len(need) == 0 || need[0].Name != b.Binlog.File {
		return nil, fmt.Errorf("binlog %s (start of recovery) is not in the archive; is `mqbackrest archive-daemon` running?", b.Binlog.File)
	}
	// Stop at the first gap: replay past it would be inconsistent.
	for i := 1; i < len(need); i++ {
		if gaps := repo.BinlogGaps(need[i-1 : i+1]); len(gaps) > 0 {
			log.Warn("binlog archive has a gap; recovery will stop before it", "missing", gaps[0])
			need = need[:i]
			break
		}
	}
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return nil, err
	}
	plan := &ReplayPlan{Backup: b.Label, StartFile: b.Binlog.File, StartPosition: b.Binlog.Position, StopDatetime: targetTime, CreatedAt: time.Now().UTC()}
	for _, bl := range need {
		log.Info("fetching binlog", "name", bl.Name)
		if err := fetchOne(ctx, r, bl, filepath.Join(dir, bl.Name)); err != nil {
			return nil, err
		}
		plan.Files = append(plan.Files, bl.Name)
	}
	pj, _ := json.MarshalIndent(plan, "", "  ")
	return plan, os.WriteFile(filepath.Join(dir, "replay.json"), pj, 0o640)
}

func fetchOne(ctx context.Context, r *repo.Repo, bl *repo.BinlogMeta, path string) error {
	rc, err := r.GetBinlog(ctx, bl)
	if err != nil {
		return err
	}
	f, err := os.Create(path)
	if err != nil {
		rc.Close()
		return err
	}
	_, err = io.Copy(f, rc)
	if cerr := rc.Close(); err == nil {
		err = cerr
	}
	if cerr := f.Close(); err == nil {
		err = cerr
	}
	return err
}
