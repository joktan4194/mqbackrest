package repo

import (
	"bytes"
	"context"
	"io"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/joktan4194/mqbackrest/internal/config"
	"github.com/joktan4194/mqbackrest/internal/storage"
)

func newRepo(t *testing.T, pass string) *Repo {
	t.Helper()
	cfg, err := config.Parse([]byte("stanza: t\nrepo:\n  path: " + t.TempDir() + "\n  cipher_pass: '" + pass + "'\n"))
	if err != nil {
		t.Fatal(err)
	}
	st, err := storage.NewLocal(cfg.Repo.Path)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if _, err := Create(ctx, st, cfg); err != nil {
		t.Fatal(err)
	}
	if _, err := Create(ctx, st, cfg); err == nil {
		t.Fatal("second stanza-create must fail")
	}
	r, err := Open(ctx, st, cfg)
	if err != nil {
		t.Fatal(err)
	}
	return r
}

func TestBackupRoundTripAndCorruption(t *testing.T) {
	for _, pass := range []string{"", "hunter2"} {
		r := newRepo(t, pass)
		ctx := context.Background()
		data := bytes.Repeat([]byte("innodb page "), 10_000)
		p, err := r.PutPart(ctx, "20260101-000000F", 0, bytes.NewReader(data))
		if err != nil {
			t.Fatal(err)
		}
		m := &Manifest{Label: "20260101-000000F", Type: TypeFull, StartTime: time.Now(), Parts: []Part{p}}
		if err := r.SaveManifest(ctx, m); err != nil {
			t.Fatal(err)
		}
		set, err := r.ListBackups(ctx)
		if err != nil || len(set.Backups) != 1 {
			t.Fatalf("list: %v %d", err, len(set.Backups))
		}
		got := set.Backups[0]
		rc, err := r.GetPart(ctx, got, got.Parts[0])
		if err != nil {
			t.Fatal(err)
		}
		plain, _ := io.ReadAll(rc)
		if err := rc.Close(); err != nil || !bytes.Equal(plain, data) {
			t.Fatalf("round trip failed: %v", err)
		}
		if strings.HasSuffix(p.Name, ".enc") != (pass != "") {
			t.Fatalf("unexpected part name %s for pass %q", p.Name, pass)
		}

		// Corrupt the manifest's view of the checksum: Close must fail.
		bad := got.Parts[0]
		bad.SHA256 = strings.Repeat("0", 64)
		rc, _ = r.GetPart(ctx, got, bad)
		io.ReadAll(rc)
		if err := rc.Close(); err == nil {
			t.Fatal("expected checksum mismatch")
		}
	}
}

func TestOpenRejectsWrongPassphrase(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	cfg, _ := config.Parse([]byte("stanza: t\nrepo:\n  path: " + dir + "\n  cipher_pass: right\n"))
	st, _ := storage.NewLocal(dir)
	if _, err := Create(ctx, st, cfg); err != nil {
		t.Fatal(err)
	}
	cfg.Repo.CipherPass = "wrong"
	if _, err := Open(ctx, st, cfg); err == nil {
		t.Fatal("expected wrong passphrase error")
	}
	cfg.Repo.CipherPass = ""
	if _, err := Open(ctx, st, cfg); err == nil {
		t.Fatal("expected missing passphrase error")
	}
}

func TestOrphansDetected(t *testing.T) {
	r := newRepo(t, "")
	ctx := context.Background()
	if _, err := r.PutPart(ctx, "20260101-000000F", 0, strings.NewReader("x")); err != nil {
		t.Fatal(err)
	}
	set, _ := r.ListBackups(ctx)
	if len(set.Backups) != 0 || !reflect.DeepEqual(set.Orphans, []string{"20260101-000000F"}) {
		t.Fatalf("orphan detection: %+v", set)
	}
	if err := r.DeleteBackup(ctx, "20260101-000000F"); err != nil {
		t.Fatal(err)
	}
	set, _ = r.ListBackups(ctx)
	if len(set.Orphans) != 0 {
		t.Fatal("orphan not deleted")
	}
}

func mk(label string, typ, prior string, at int, binlog string) *Manifest {
	m := &Manifest{Label: label, Type: typ, Prior: prior, StartTime: time.Unix(int64(at), 0), StopTime: time.Unix(int64(at)+1, 0)}
	if binlog != "" {
		m.Binlog = &BinlogPos{File: binlog, Position: 4}
	}
	return m
}

func TestChainAndExpire(t *testing.T) {
	backups := []*Manifest{
		mk("A", TypeFull, "", 1, "binlog.000001"),
		mk("A_B", TypeIncr, "A", 2, "binlog.000002"),
		mk("C", TypeFull, "", 3, "binlog.000003"),
		mk("C_D", TypeIncr, "C", 4, "binlog.000004"),
		mk("C_E", TypeIncr, "C_D", 5, "binlog.000005"),
		mk("F", TypeFull, "", 6, "binlog.000006"),
	}
	set := &BackupSet{Backups: backups}
	chain, err := set.Chain("C_E")
	if err != nil {
		t.Fatal(err)
	}
	var labels []string
	for _, c := range chain {
		labels = append(labels, c.Label)
	}
	if !reflect.DeepEqual(labels, []string{"C", "C_D", "C_E"}) {
		t.Fatalf("chain = %v", labels)
	}
	if _, err := set.Chain("missing"); err == nil {
		t.Fatal("expected error for missing backup")
	}

	var bls []*BinlogMeta
	for i := 1; i <= 7; i++ {
		bls = append(bls, &BinlogMeta{Name: BinlogName{"binlog", uint64(i), 6}.String()})
	}
	plan := PlanExpire(backups, bls, 2)
	if !reflect.DeepEqual(plan.Backups, []string{"A", "A_B"}) {
		t.Fatalf("expire backups = %v", plan.Backups)
	}
	if !reflect.DeepEqual(plan.Binlogs, []string{"binlog.000001", "binlog.000002"}) {
		t.Fatalf("expire binlogs = %v", plan.Binlogs)
	}
	if p := PlanExpire(backups, bls, 10); len(p.Backups)+len(p.Binlogs) != 0 {
		t.Fatalf("nothing should expire: %+v", p)
	}
}

func TestBinlogNames(t *testing.T) {
	n, ok := ParseBinlogName("mysql-bin.000099")
	if !ok || n.Base != "mysql-bin" || n.Seq != 99 || n.Next().String() != "mysql-bin.000100" {
		t.Fatalf("parse: %+v %v", n, ok)
	}
	for _, bad := range []string{"binlog.index", "binlog", ".000001", "binlog."} {
		if _, ok := ParseBinlogName(bad); ok {
			t.Errorf("%q should not parse", bad)
		}
	}
	bls := []*BinlogMeta{{Name: "b.000001"}, {Name: "b.000002"}, {Name: "b.000005"}}
	if g := BinlogGaps(bls); !reflect.DeepEqual(g, []string{"b.000003", "b.000004"}) {
		t.Fatalf("gaps = %v", g)
	}
}

func TestLabels(t *testing.T) {
	at := time.Date(2026, 11, 23, 10, 15, 0, 0, time.UTC)
	full := NewLabel(at, TypeFull, nil)
	if full != "20261123-101500F" {
		t.Fatal(full)
	}
	incr := NewLabel(at.Add(time.Hour), TypeIncr, &Manifest{Label: full})
	if incr != "20261123-101500F_20261123-111500I" || RootLabel(incr) != full {
		t.Fatal(incr)
	}
	incr2 := NewLabel(at.Add(2*time.Hour), TypeIncr, &Manifest{Label: incr})
	if RootLabel(incr2) != full {
		t.Fatal(incr2)
	}
}
