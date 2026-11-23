// Package cli wires mqBackRest's commands.
package cli

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/spf13/cobra"

	"github.com/joktan4194/mqbackrest/internal/config"
	"github.com/joktan4194/mqbackrest/internal/mysql"
	"github.com/joktan4194/mqbackrest/internal/repo"
	"github.com/joktan4194/mqbackrest/internal/storage"
	"github.com/joktan4194/mqbackrest/internal/xtrabackup"
)

type globals struct {
	configPath string
	logLevel   string
	logFormat  string
	log        *slog.Logger
}

var g globals

// Execute runs the CLI and returns the process exit code.
func Execute() int {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	root := &cobra.Command{
		Use:           "mqbackrest",
		Short:         "Reliable, parallel backup and point-in-time recovery for MySQL",
		SilenceUsage:  true,
		SilenceErrors: true,
		PersistentPreRunE: func(cmd *cobra.Command, _ []string) error {
			var lvl slog.Level
			if err := lvl.UnmarshalText([]byte(g.logLevel)); err != nil {
				return fmt.Errorf("--log-level: %w", err)
			}
			opts := &slog.HandlerOptions{Level: lvl}
			var h slog.Handler = slog.NewTextHandler(os.Stderr, opts)
			if g.logFormat == "json" {
				h = slog.NewJSONHandler(os.Stderr, opts)
			}
			g.log = slog.New(h)
			return nil
		},
	}
	def := os.Getenv("MQBACKREST_CONFIG")
	if def == "" {
		def = "/etc/mqbackrest/mqbackrest.yaml"
	}
	root.PersistentFlags().StringVarP(&g.configPath, "config", "c", def, "config file (env MQBACKREST_CONFIG)")
	root.PersistentFlags().StringVar(&g.logLevel, "log-level", "info", "debug, info, warn or error")
	root.PersistentFlags().StringVar(&g.logFormat, "log-format", "text", "text or json")
	root.AddCommand(stanzaCreateCmd(), checkCmd(), backupCmd(), archiveDaemonCmd(), restoreCmd(),
		replayCmd(), infoCmd(), verifyCmd(), expireCmd(), versionCmd())
	if err := root.ExecuteContext(ctx); err != nil {
		if g.log != nil {
			g.log.Error(err.Error())
		} else {
			fmt.Fprintln(os.Stderr, "error:", err)
		}
		return 1
	}
	return 0
}

type env struct {
	cfg  *config.Config
	st   storage.Storage
	repo *repo.Repo
}

func loadEnv(ctx context.Context, openRepo bool) (*env, error) {
	cfg, err := config.Load(g.configPath)
	if err != nil {
		return nil, err
	}
	st, err := storage.Open(cfg.Repo)
	if err != nil {
		return nil, err
	}
	e := &env{cfg: cfg, st: st}
	if openRepo {
		if e.repo, err = repo.Open(ctx, st, cfg); err != nil {
			return nil, err
		}
	}
	return e, nil
}

// runner builds an xtrabackup runner; the caller must invoke cleanup.
func (e *env) runner(withCreds bool) (*xtrabackup.Runner, func(), error) {
	xb := &xtrabackup.Runner{
		Bin: e.cfg.XtraBackup.Bin, XbstreamBin: e.cfg.XtraBackup.XbstreamBin,
		Datadir: e.cfg.MySQL.Datadir, Parallel: e.cfg.XtraBackup.Parallel,
		BackupArgs: e.cfg.XtraBackup.ExtraArgs, PrepareArgs: e.cfg.XtraBackup.PrepareArgs,
		RestoreArgs: e.cfg.XtraBackup.RestoreArgs, Log: g.log,
	}
	if !withCreds {
		return xb, func() {}, nil
	}
	p, cleanup, err := mysql.DefaultsFile(e.cfg.MySQL)
	if err != nil {
		return nil, nil, err
	}
	xb.DefaultsFile = p
	return xb, cleanup, nil
}
