package restore

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
)

// ReplayOptions configures `mqbackrest replay`.
type ReplayOptions struct {
	PITRDir        string
	MysqlbinlogBin string
	MysqlBin       string
	DefaultsFile   string
	DryRun         bool
}

// Replay pipes archived binlogs through mysqlbinlog into a running server.
func Replay(ctx context.Context, o ReplayOptions, log *slog.Logger) error {
	raw, err := os.ReadFile(filepath.Join(o.PITRDir, "replay.json"))
	if err != nil {
		return fmt.Errorf("read replay plan: %w", err)
	}
	var plan ReplayPlan
	if err := json.Unmarshal(raw, &plan); err != nil {
		return err
	}
	args := []string{fmt.Sprintf("--start-position=%d", plan.StartPosition)}
	if plan.StopDatetime != "" {
		args = append(args, "--stop-datetime="+plan.StopDatetime)
	}
	for _, f := range plan.Files {
		args = append(args, filepath.Join(o.PITRDir, f))
	}
	mysqlArgs := []string{}
	if o.DefaultsFile != "" {
		mysqlArgs = append(mysqlArgs, "--defaults-extra-file="+o.DefaultsFile)
	}
	if o.DryRun {
		fmt.Printf("%s %s | %s %s\n", o.MysqlbinlogBin, strings.Join(args, " "), o.MysqlBin, strings.Join(mysqlArgs, " "))
		return nil
	}
	log.Info("replaying binlogs", "files", len(plan.Files), "start", plan.StartFile, "position", plan.StartPosition, "stop", plan.StopDatetime)

	dump := exec.CommandContext(ctx, o.MysqlbinlogBin, args...)
	load := exec.CommandContext(ctx, o.MysqlBin, mysqlArgs...)
	pr, pw := io.Pipe()
	dump.Stdout, dump.Stderr = pw, os.Stderr
	load.Stdin, load.Stdout, load.Stderr = pr, os.Stdout, os.Stderr
	if err := load.Start(); err != nil {
		return err
	}
	if err := dump.Start(); err != nil {
		pw.Close()
		load.Wait()
		return err
	}
	dumpErr := dump.Wait()
	pw.CloseWithError(dumpErr)
	loadErr := load.Wait()
	if dumpErr != nil {
		return fmt.Errorf("mysqlbinlog: %w", dumpErr)
	}
	if loadErr != nil {
		return fmt.Errorf("mysql: %w", loadErr)
	}
	log.Info("replay complete")
	return nil
}
