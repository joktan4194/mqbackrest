// Package binlog continuously archives MySQL binary logs for point-in-time recovery.
//
// It runs `mysqlbinlog --read-from-remote-server --raw --stop-never`, which
// connects like a replica and mirrors binlogs into a local spool directory.
// Every file except the newest is complete; those are pushed to the
// repository and removed from the spool.
package binlog

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"time"

	"github.com/joktan4194/mqbackrest/internal/mysql"
	"github.com/joktan4194/mqbackrest/internal/repo"
)

// Archiver mirrors and uploads binlogs.
type Archiver struct {
	Repo           *repo.Repo
	DB             *sql.DB
	MysqlbinlogBin string
	DefaultsFile   string
	SpoolDir       string
	ServerID       int
	PollInterval   time.Duration
	Log            *slog.Logger
}

// Run blocks until ctx is cancelled, restarting mysqlbinlog on failure.
func (a *Archiver) Run(ctx context.Context) error {
	if err := os.MkdirAll(a.SpoolDir, 0o750); err != nil {
		return err
	}
	backoff := time.Second
	for {
		start, err := a.startFile(ctx)
		if err == nil {
			a.Log.Info("streaming binlogs", "from", start, "spool", a.SpoolDir)
			err = a.stream(ctx, start)
		}
		if ctx.Err() != nil {
			a.Log.Info("archive-daemon stopping; pushing completed files")
			return a.sync(context.Background())
		}
		a.Log.Error("binlog streaming interrupted; retrying", "err", err, "in", backoff)
		select {
		case <-time.After(backoff):
		case <-ctx.Done():
			return a.sync(context.Background())
		}
		backoff = min(backoff*2, time.Minute)
	}
}

// startFile decides where to resume streaming.
func (a *Archiver) startFile(ctx context.Context) (string, error) {
	if err := a.sync(ctx); err != nil {
		return "", err
	}
	spool, err := a.spoolFiles()
	if err != nil {
		return "", err
	}
	server, err := mysql.BinaryLogs(ctx, a.DB)
	if err != nil {
		return "", fmt.Errorf("SHOW BINARY LOGS (is log_bin enabled?): %w", err)
	}
	if len(server) == 0 {
		return "", fmt.Errorf("server reports no binary logs; enable log_bin")
	}
	var want string
	if len(spool) > 0 {
		want = spool[len(spool)-1] // partial file: re-fetch from the start
	} else {
		archived, err := a.Repo.ListBinlogs(ctx)
		if err != nil {
			return "", err
		}
		if len(archived) == 0 {
			return server[0], nil
		}
		last, _ := repo.ParseBinlogName(archived[len(archived)-1].Name)
		want = last.Next().String()
	}
	for _, s := range server {
		if s == want {
			return want, nil
		}
	}
	// The file we need was purged before we could fetch it: that is a gap.
	w, _ := repo.ParseBinlogName(want)
	for _, s := range server {
		if n, ok := repo.ParseBinlogName(s); ok && n.Base == w.Base && n.Seq > w.Seq {
			a.Log.Error("binlog GAP: required file was purged from the server; point-in-time recovery across it is impossible", "missing_from", want, "resuming_at", s)
			return s, nil
		}
	}
	return "", fmt.Errorf("binlog %s not yet available on server", want)
}

func (a *Archiver) stream(ctx context.Context, start string) error {
	sctx, cancel := context.WithCancel(ctx)
	defer cancel()
	args := []string{}
	if a.DefaultsFile != "" {
		args = append(args, "--defaults-extra-file="+a.DefaultsFile)
	}
	args = append(args, "--read-from-remote-server", "--raw", "--stop-never", "--verify-binlog-checksum",
		fmt.Sprintf("--connection-server-id=%d", a.ServerID),
		"--result-file="+filepath.Clean(a.SpoolDir)+string(filepath.Separator), start)
	cmd := exec.CommandContext(sctx, a.MysqlbinlogBin, args...)
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		return err
	}
	exited := make(chan error, 1)
	go func() { exited <- cmd.Wait() }()
	t := time.NewTicker(a.PollInterval)
	defer t.Stop()
	for {
		select {
		case err := <-exited:
			if err == nil {
				err = fmt.Errorf("mysqlbinlog exited")
			}
			return err
		case <-t.C:
			if err := a.sync(ctx); err != nil {
				a.Log.Error("push failed; will retry", "err", err)
			}
		case <-ctx.Done():
			cancel()
			<-exited
			return ctx.Err()
		}
	}
}

// sync pushes every complete spool file (all but the newest).
func (a *Archiver) sync(ctx context.Context) error {
	files, err := a.spoolFiles()
	if err != nil {
		return err
	}
	for i := 0; i < len(files)-1; i++ {
		if err := a.push(ctx, files[i]); err != nil {
			return err
		}
	}
	return nil
}

func (a *Archiver) push(ctx context.Context, name string) error {
	p := filepath.Join(a.SpoolDir, name)
	f, err := os.Open(p)
	if err != nil {
		return err
	}
	meta, err := a.Repo.PutBinlog(ctx, name, f)
	f.Close()
	if err != nil {
		return fmt.Errorf("push %s: %w", name, err)
	}
	a.Log.Info("binlog archived", "name", name, "raw", meta.RawSize, "stored", meta.Size)
	return os.Remove(p)
}

// spoolFiles lists binlog files in the spool, ordered by sequence.
func (a *Archiver) spoolFiles() ([]string, error) {
	entries, err := os.ReadDir(a.SpoolDir)
	if err != nil {
		return nil, err
	}
	var names []repo.BinlogName
	for _, e := range entries {
		if n, ok := repo.ParseBinlogName(e.Name()); ok && !e.IsDir() {
			names = append(names, n)
		}
	}
	sort.Slice(names, func(i, j int) bool { return names[i].Seq < names[j].Seq })
	out := make([]string, len(names))
	for i, n := range names {
		out[i] = n.String()
	}
	return out, nil
}
