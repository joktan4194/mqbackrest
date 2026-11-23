// Package xtrabackup drives Percona XtraBackup and xbstream.
package xtrabackup

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"log/slog"
	"os/exec"
	"regexp"
	"strconv"
	"strings"
	"sync"
)

// Runner invokes the xtrabackup toolchain.
type Runner struct {
	Bin          string
	XbstreamBin  string
	DefaultsFile string
	Datadir      string
	Parallel     int
	BackupArgs   []string
	PrepareArgs  []string
	RestoreArgs  []string
	Log          *slog.Logger
}

// Version returns `xtrabackup --version` output.
func (r *Runner) Version(ctx context.Context) (string, error) {
	out, err := exec.CommandContext(ctx, r.Bin, "--version").CombinedOutput()
	if err != nil {
		return "", fmt.Errorf("%s --version: %w: %s", r.Bin, err, out)
	}
	for _, line := range strings.Split(string(out), "\n") {
		if strings.Contains(line, "version") {
			return strings.TrimSpace(line), nil
		}
	}
	return strings.TrimSpace(string(out)), nil
}

// Backup streams an xbstream archive to out. lsnDir receives
// xtrabackup_checkpoints and xtrabackup_info. incrementalLSN "" means full.
func (r *Runner) Backup(ctx context.Context, lsnDir, incrementalLSN string, out io.Writer) error {
	args := []string{}
	if r.DefaultsFile != "" {
		args = append(args, "--defaults-extra-file="+r.DefaultsFile) // must be first
	}
	args = append(args, "--backup", "--stream=xbstream",
		"--target-dir="+lsnDir, "--extra-lsndir="+lsnDir,
		fmt.Sprintf("--parallel=%d", r.Parallel))
	if r.Datadir != "" {
		args = append(args, "--datadir="+r.Datadir)
	}
	if incrementalLSN != "" {
		args = append(args, "--incremental-lsn="+incrementalLSN)
	}
	args = append(args, r.BackupArgs...)
	return r.run(ctx, r.Bin, args, nil, out, true)
}

// Extract unpacks an xbstream read from in into dir.
func (r *Runner) Extract(ctx context.Context, dir string, in io.Reader) error {
	return r.run(ctx, r.XbstreamBin, []string{"-x", "-C", dir, fmt.Sprintf("--parallel=%d", r.Parallel)}, in, nil, false)
}

// Prepare applies redo logs. incDir merges an incremental into target.
func (r *Runner) Prepare(ctx context.Context, target, incDir string, applyLogOnly bool) error {
	args := []string{"--prepare", "--target-dir=" + target}
	if applyLogOnly {
		args = append(args, "--apply-log-only")
	}
	if incDir != "" {
		args = append(args, "--incremental-dir="+incDir)
	}
	args = append(args, r.PrepareArgs...)
	return r.run(ctx, r.Bin, args, nil, nil, true)
}

// MoveBack moves a prepared backup into an empty datadir.
func (r *Runner) MoveBack(ctx context.Context, target, datadir string) error {
	args := append([]string{"--move-back", "--target-dir=" + target, "--datadir=" + datadir}, r.RestoreArgs...)
	return r.run(ctx, r.Bin, args, nil, nil, true)
}

func (r *Runner) run(ctx context.Context, bin string, args []string, in io.Reader, out io.Writer, wantOK bool) error {
	cmd := exec.CommandContext(ctx, bin, args...)
	cmd.Stdin, cmd.Stdout = in, out
	stderr, err := cmd.StderrPipe()
	if err != nil {
		return err
	}
	r.Log.Debug("exec", "cmd", bin, "args", strings.Join(args, " "))
	if err := cmd.Start(); err != nil {
		return fmt.Errorf("start %s: %w", bin, err)
	}
	tail := &tailBuffer{max: 40}
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		sc := bufio.NewScanner(stderr)
		sc.Buffer(make([]byte, 64*1024), 1024*1024)
		for sc.Scan() {
			tail.add(sc.Text())
			r.Log.Debug(bin, "line", sc.Text())
		}
	}()
	wg.Wait()
	err = cmd.Wait()
	if err == nil && wantOK && !tail.contains("completed OK!") {
		err = fmt.Errorf("did not report 'completed OK!'")
	}
	if err != nil {
		return fmt.Errorf("%s failed: %w\n--- last stderr lines ---\n%s", bin, err, tail.String())
	}
	return nil
}

type tailBuffer struct {
	mu    sync.Mutex
	max   int
	lines []string
}

func (t *tailBuffer) add(s string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.lines = append(t.lines, s)
	if len(t.lines) > t.max {
		t.lines = t.lines[1:]
	}
}

func (t *tailBuffer) contains(s string) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	for _, l := range t.lines {
		if strings.Contains(l, s) {
			return true
		}
	}
	return false
}

func (t *tailBuffer) String() string {
	t.mu.Lock()
	defer t.mu.Unlock()
	return strings.Join(t.lines, "\n")
}

// Checkpoints is parsed xtrabackup_checkpoints.
type Checkpoints struct {
	BackupType string
	FromLSN    uint64
	ToLSN      uint64
	LastLSN    uint64
}

func kv(text string) map[string]string {
	m := map[string]string{}
	for _, line := range strings.Split(text, "\n") {
		k, v, ok := strings.Cut(line, "=")
		if ok {
			m[strings.TrimSpace(k)] = strings.TrimSpace(v)
		}
	}
	return m
}

// ParseCheckpoints parses the xtrabackup_checkpoints file.
func ParseCheckpoints(text string) (Checkpoints, error) {
	m := kv(text)
	var c Checkpoints
	c.BackupType = m["backup_type"]
	var err error
	for k, dst := range map[string]*uint64{"from_lsn": &c.FromLSN, "to_lsn": &c.ToLSN, "last_lsn": &c.LastLSN} {
		if m[k] == "" {
			return c, fmt.Errorf("xtrabackup_checkpoints missing %s", k)
		}
		if *dst, err = strconv.ParseUint(m[k], 10, 64); err != nil {
			return c, fmt.Errorf("xtrabackup_checkpoints %s: %w", k, err)
		}
	}
	return c, nil
}

// Info is the subset of xtrabackup_info mqBackRest records.
type Info struct {
	ServerVersion string
	ToolVersion   string
	BinlogFile    string
	BinlogPos     uint64
	GTIDSet       string
}

var binlogPosRe = regexp.MustCompile(`(?s)filename '([^']+)', position '(\d+)'(?:, GTID of the last change '([^']*)')?`)

// ParseInfo parses the xtrabackup_info file.
func ParseInfo(text string) Info {
	m := kv(text)
	inf := Info{ServerVersion: m["server_version"], ToolVersion: m["tool_version"]}
	if sm := binlogPosRe.FindStringSubmatch(text); sm != nil {
		inf.BinlogFile = sm[1]
		inf.BinlogPos, _ = strconv.ParseUint(sm[2], 10, 64)
		inf.GTIDSet = strings.Join(strings.Fields(sm[3]), "") // GTID sets may wrap across lines
	}
	return inf
}
