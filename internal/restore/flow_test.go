package restore_test

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/joktan4194/mqbackrest/internal/backup"
	"github.com/joktan4194/mqbackrest/internal/config"
	"github.com/joktan4194/mqbackrest/internal/repo"
	"github.com/joktan4194/mqbackrest/internal/restore"
	"github.com/joktan4194/mqbackrest/internal/storage"
	"github.com/joktan4194/mqbackrest/internal/xtrabackup"
)

// Fake xtrabackup: emits 3MB of random "xbstream" (copied to $FAKE_DIR for
// comparison) and writes checkpoint/info files like the real tool.
const fakeXtrabackup = `#!/bin/sh
echo "$@" >> "$FAKE_DIR/calls.log"
case " $* " in
*" --backup "*)
  inc=0
  for a in "$@"; do case $a in --extra-lsndir=*) d=${a#*=};; --incremental-lsn=*) inc=${a#*=};; esac; done
  to=$((inc+1000))
  printf 'backup_type = x\nfrom_lsn = %s\nto_lsn = %s\nlast_lsn = %s\n' $inc $to $to > "$d/xtrabackup_checkpoints"
  printf "server_version = 8.0.40\nbinlog_pos = filename 'binlog.000003', position '157'\n" > "$d/xtrabackup_info"
  head -c 3000000 /dev/urandom | tee "$FAKE_DIR/sent-$to"
  ;;
esac
echo "completed OK!" >&2
`

// Fake xbstream: "-x -C <dir>" stores the stream verbatim.
const fakeXbstream = `#!/bin/sh
cat > "$3/stream.bin"
`

func TestBackupIncrementalRestorePITR(t *testing.T) {
	ctx := context.Background()
	fake := t.TempDir()
	t.Setenv("FAKE_DIR", fake)
	xbBin, xbsBin := filepath.Join(fake, "xtrabackup"), filepath.Join(fake, "xbstream")
	os.WriteFile(xbBin, []byte(fakeXtrabackup), 0o755)
	os.WriteFile(xbsBin, []byte(fakeXbstream), 0o755)

	cfg, err := config.Parse([]byte("stanza: e2e\nrepo:\n  path: " + filepath.Join(fake, "repo") + "\n  cipher_pass: pw\n"))
	if err != nil {
		t.Fatal(err)
	}
	st, _ := storage.NewLocal(cfg.Repo.Path)
	if _, err := repo.Create(ctx, st, cfg); err != nil {
		t.Fatal(err)
	}
	r, err := repo.Open(ctx, st, cfg)
	if err != nil {
		t.Fatal(err)
	}
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	xb := &xtrabackup.Runner{Bin: xbBin, XbstreamBin: xbsBin, Parallel: 2, Log: log}
	opts := backup.Options{ProcessMax: 3, PartSize: 1 << 20}

	opts.Type = repo.TypeIncr // no prior: must fall back to full
	full, err := backup.Run(ctx, r, xb, opts, log)
	if err != nil {
		t.Fatal(err)
	}
	if full.Type != repo.TypeFull || len(full.Parts) != 3 || full.ToLSN != 1000 {
		t.Fatalf("full: type=%s parts=%d to_lsn=%d", full.Type, len(full.Parts), full.ToLSN)
	}
	incr, err := backup.Run(ctx, r, xb, opts, log)
	if err != nil {
		t.Fatal(err)
	}
	if incr.Type != repo.TypeIncr || incr.Prior != full.Label || incr.FromLSN != 1000 || incr.ToLSN != 2000 {
		t.Fatalf("incr: %+v", incr)
	}

	for _, n := range []string{"binlog.000003", "binlog.000004"} {
		if _, err := r.PutBinlog(ctx, n, strings.NewReader("events of "+n)); err != nil {
			t.Fatal(err)
		}
	}

	target := filepath.Join(fake, "datadir")
	pitr := filepath.Join(fake, "pitr")
	err = restore.Run(ctx, r, xb, restore.Options{
		Set: "latest", TargetDir: target, PITRDir: pitr, KeepWork: true,
		TargetTime: time.Now().Add(time.Hour).Format(restore.TimeLayout),
	}, log)
	if err != nil {
		t.Fatal(err)
	}

	// Each backup's stream must arrive byte-for-byte at xbstream.
	work := filepath.Join(fake, ".mqbackrest-restore-"+incr.Label)
	for i, m := range []*repo.Manifest{full, incr} {
		got, _ := os.ReadFile(filepath.Join(work, filepathFor(i, m.Label), "stream.bin"))
		want, _ := os.ReadFile(filepath.Join(fake, "sent-"+itoa(m.ToLSN)))
		if len(want) == 0 || !bytes.Equal(got, want) {
			t.Fatalf("%s: restored stream differs (%d vs %d bytes)", m.Label, len(got), len(want))
		}
	}

	calls, _ := os.ReadFile(filepath.Join(fake, "calls.log"))
	lines := strings.Split(strings.TrimSpace(string(calls)), "\n")
	tail := lines[len(lines)-3:]
	if !strings.Contains(tail[0], "--prepare") || !strings.Contains(tail[0], "--apply-log-only") ||
		!strings.Contains(tail[1], "--incremental-dir=") || strings.Contains(tail[1], "--apply-log-only") ||
		!strings.Contains(tail[2], "--move-back") {
		t.Fatalf("unexpected prepare sequence:\n%s", strings.Join(tail, "\n"))
	}

	var plan restore.ReplayPlan
	raw, err := os.ReadFile(filepath.Join(pitr, "replay.json"))
	if err != nil {
		t.Fatal(err)
	}
	json.Unmarshal(raw, &plan)
	if plan.StartFile != "binlog.000003" || plan.StartPosition != 157 || len(plan.Files) != 2 {
		t.Fatalf("replay plan: %+v", plan)
	}
	if b, _ := os.ReadFile(filepath.Join(pitr, "binlog.000004")); string(b) != "events of binlog.000004" {
		t.Fatalf("binlog content: %q", b)
	}

	// Restoring into a non-empty dir must be refused.
	if err := restore.Run(ctx, r, xb, restore.Options{Set: "latest", TargetDir: pitr}, log); err == nil {
		t.Fatal("expected refusal for non-empty target dir")
	}
}

func filepathFor(i int, label string) string { return "0" + itoa(uint64(i)) + "-" + label }

func itoa(n uint64) string {
	b, _ := json.Marshal(n)
	return string(b)
}
