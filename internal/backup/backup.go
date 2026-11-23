// Package backup orchestrates `mqbackrest backup`.
package backup

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"sync"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/joktan4194/mqbackrest/internal/repo"
	"github.com/joktan4194/mqbackrest/internal/version"
	"github.com/joktan4194/mqbackrest/internal/xtrabackup"
)

// Options for a backup run.
type Options struct {
	Type       string // repo.TypeFull or repo.TypeIncr
	ProcessMax int
	PartSize   int
}

// Run takes a backup and returns its manifest.
func Run(ctx context.Context, r *repo.Repo, xb *xtrabackup.Runner, o Options, log *slog.Logger) (*repo.Manifest, error) {
	set, err := r.ListBackups(ctx)
	if err != nil {
		return nil, err
	}
	typ, prior := o.Type, set.Latest()
	if typ == repo.TypeIncr && prior == nil {
		log.Warn("no prior backup exists; taking a full backup instead of incr")
		typ = repo.TypeFull
	}
	if typ == repo.TypeFull {
		prior = nil
	}

	start := time.Now().UTC()
	m := &repo.Manifest{
		Label: repo.NewLabel(start, typ, prior), Type: typ, StartTime: start,
		MQBackRestVersion: version.Version, Compress: r.Stanza.Compress, Encrypted: r.Encrypted(),
	}
	incrLSN := ""
	if prior != nil {
		m.Prior = prior.Label
		incrLSN = strconv.FormatUint(prior.ToLSN, 10)
	}
	log.Info("backup start", "label", m.Label, "type", typ, "prior", m.Prior, "incremental_lsn", incrLSN)

	lsnDir, err := os.MkdirTemp("", "mqbackrest-lsn-")
	if err != nil {
		return nil, err
	}
	defer os.RemoveAll(lsnDir)

	parts, err := stream(ctx, r, xb, m.Label, lsnDir, incrLSN, o, log)
	if err != nil {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cancel()
		if derr := r.DeleteBackup(cleanupCtx, m.Label); derr != nil {
			log.Warn("could not remove partial backup; `mqbackrest expire` will clean it up", "label", m.Label, "err", derr)
		}
		return nil, err
	}
	m.Parts = parts
	for _, p := range parts {
		m.RawSize += p.RawSize
		m.Size += p.Size
	}

	cp, err := os.ReadFile(filepath.Join(lsnDir, "xtrabackup_checkpoints"))
	if err != nil {
		return nil, fmt.Errorf("read xtrabackup_checkpoints: %w", err)
	}
	ck, err := xtrabackup.ParseCheckpoints(string(cp))
	if err != nil {
		return nil, err
	}
	m.Checkpoints, m.FromLSN, m.ToLSN, m.LastLSN = string(cp), ck.FromLSN, ck.ToLSN, ck.LastLSN
	if infoText, err := os.ReadFile(filepath.Join(lsnDir, "xtrabackup_info")); err == nil {
		inf := xtrabackup.ParseInfo(string(infoText))
		m.Info, m.ServerVersion, m.ToolVersion = string(infoText), inf.ServerVersion, inf.ToolVersion
		if inf.BinlogFile != "" {
			m.Binlog = &repo.BinlogPos{File: inf.BinlogFile, Position: inf.BinlogPos, GTIDSet: inf.GTIDSet}
		}
	}
	if m.Binlog == nil {
		log.Warn("backup has no binlog position; point-in-time recovery from it will not be possible (is log_bin enabled?)")
	}
	m.StopTime = time.Now().UTC()
	if err := r.SaveManifest(ctx, m); err != nil {
		return nil, fmt.Errorf("save manifest: %w", err)
	}
	log.Info("backup complete", "label", m.Label, "parts", len(parts),
		"raw", HumanBytes(m.RawSize), "stored", HumanBytes(m.Size), "duration", m.StopTime.Sub(m.StartTime).Round(time.Second))
	return m, nil
}

// stream runs xtrabackup and uploads its output as fixed-size parts in parallel.
func stream(ctx context.Context, r *repo.Repo, xb *xtrabackup.Runner, label, lsnDir, incrLSN string, o Options, log *slog.Logger) ([]repo.Part, error) {
	g, gctx := errgroup.WithContext(ctx)
	pr, pw := io.Pipe()

	g.Go(func() error {
		err := xb.Backup(gctx, lsnDir, incrLSN, pw)
		pw.CloseWithError(err)
		return err
	})

	type job struct {
		idx int
		buf []byte
	}
	jobs := make(chan job)
	// Bounded buffer pool: memory use is (ProcessMax+1) * PartSize.
	pool := make(chan []byte, o.ProcessMax+1)
	for i := 0; i < cap(pool); i++ {
		pool <- make([]byte, o.PartSize)
	}

	g.Go(func() error {
		defer close(jobs)
		for idx := 0; ; idx++ {
			var buf []byte
			select {
			case buf = <-pool:
			case <-gctx.Done():
				pr.CloseWithError(gctx.Err())
				return gctx.Err()
			}
			n, err := io.ReadFull(pr, buf)
			if n > 0 {
				select {
				case jobs <- job{idx, buf[:n]}:
				case <-gctx.Done():
					return gctx.Err()
				}
			}
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				return nil
			}
			if err != nil {
				return err
			}
		}
	})

	var mu sync.Mutex
	var parts []repo.Part
	for w := 0; w < o.ProcessMax; w++ {
		g.Go(func() error {
			for j := range jobs {
				p, err := r.PutPart(gctx, label, j.idx, bytes.NewReader(j.buf))
				pool <- j.buf[:cap(j.buf)]
				if err != nil {
					return err
				}
				log.Debug("part stored", "idx", j.idx, "raw", p.RawSize, "stored", p.Size)
				mu.Lock()
				parts = append(parts, p)
				mu.Unlock()
			}
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return nil, err
	}
	if len(parts) == 0 {
		return nil, errors.New("xtrabackup produced no output")
	}
	sort.Slice(parts, func(i, j int) bool { return parts[i].Name < parts[j].Name })
	return parts, nil
}

// HumanBytes formats a byte count.
func HumanBytes(n int64) string {
	const unit = 1024
	if n < unit {
		return fmt.Sprintf("%dB", n)
	}
	div, exp := int64(unit), 0
	for m := n / unit; m >= unit; m /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f%ciB", float64(n)/float64(div), "KMGTPE"[exp])
}
