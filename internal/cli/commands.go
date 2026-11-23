package cli

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/spf13/cobra"

	"github.com/joktan4194/mqbackrest/internal/backup"
	"github.com/joktan4194/mqbackrest/internal/binlog"
	"github.com/joktan4194/mqbackrest/internal/lock"
	"github.com/joktan4194/mqbackrest/internal/mysql"
	"github.com/joktan4194/mqbackrest/internal/repo"
	"github.com/joktan4194/mqbackrest/internal/restore"
	"github.com/joktan4194/mqbackrest/internal/version"
)

func stanzaCreateCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "stanza-create",
		Short: "Initialise the repository for this stanza",
		RunE: func(cmd *cobra.Command, _ []string) error {
			e, err := loadEnv(cmd.Context(), false)
			if err != nil {
				return err
			}
			s, err := repo.Create(cmd.Context(), e.st, e.cfg)
			if err != nil {
				return err
			}
			g.log.Info("stanza created", "stanza", s.Name, "repo", e.st.String(), "compress", s.Compress, "encrypted", s.Cipher != nil)
			if s.Cipher != nil {
				g.log.Warn("store repo.cipher_pass somewhere safe: without it these backups cannot be restored")
			}
			return nil
		},
	}
}

func backupCmd() *cobra.Command {
	var typ string
	cmd := &cobra.Command{
		Use:   "backup",
		Short: "Take a full or incremental physical backup",
		RunE: func(cmd *cobra.Command, _ []string) error {
			if typ != repo.TypeFull && typ != repo.TypeIncr {
				return fmt.Errorf("--type must be full or incr")
			}
			e, err := loadEnv(cmd.Context(), true)
			if err != nil {
				return err
			}
			release, err := lock.Acquire(e.cfg.LockDir, e.cfg.Stanza, "backup")
			if err != nil {
				return err
			}
			defer release()
			xb, cleanup, err := e.runner(true)
			if err != nil {
				return err
			}
			defer cleanup()
			_, err = backup.Run(cmd.Context(), e.repo, xb, backup.Options{
				Type: typ, ProcessMax: e.cfg.ProcessMax, PartSize: e.cfg.PartSizeMB << 20,
			}, g.log)
			return err
		},
	}
	cmd.Flags().StringVar(&typ, "type", repo.TypeIncr, "full or incr (incr falls back to full when no backup exists)")
	return cmd
}

func archiveDaemonCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "archive-daemon",
		Short: "Continuously archive binary logs for point-in-time recovery",
		RunE: func(cmd *cobra.Command, _ []string) error {
			e, err := loadEnv(cmd.Context(), true)
			if err != nil {
				return err
			}
			release, err := lock.Acquire(e.cfg.LockDir, e.cfg.Stanza, "archive")
			if err != nil {
				return err
			}
			defer release()
			db, err := mysql.Connect(e.cfg.MySQL)
			if err != nil {
				return err
			}
			defer db.Close()
			cnf, cleanup, err := mysql.DefaultsFile(e.cfg.MySQL)
			if err != nil {
				return err
			}
			defer cleanup()
			a := &binlog.Archiver{
				Repo: e.repo, DB: db, MysqlbinlogBin: e.cfg.Binlog.MysqlbinlogBin, DefaultsFile: cnf,
				SpoolDir: e.cfg.Binlog.SpoolDir, ServerID: e.cfg.Binlog.ServerID,
				PollInterval: e.cfg.Binlog.PollInterval, Log: g.log,
			}
			return a.Run(cmd.Context())
		},
	}
}

func restoreCmd() *cobra.Command {
	var o restore.Options
	cmd := &cobra.Command{
		Use:   "restore",
		Short: "Restore a backup (optionally to a point in time) into an empty datadir",
		Example: `  mqbackrest restore --target-dir /var/lib/mysql
  mqbackrest restore --set 20261123-020000F --target-dir /var/lib/mysql
  mqbackrest restore --target-dir /var/lib/mysql --target-time "2026-11-23 14:30:00"`,
		RunE: func(cmd *cobra.Command, _ []string) error {
			e, err := loadEnv(cmd.Context(), true)
			if err != nil {
				return err
			}
			xb, cleanup, err := e.runner(false)
			if err != nil {
				return err
			}
			defer cleanup()
			return restore.Run(cmd.Context(), e.repo, xb, o, g.log)
		},
	}
	f := cmd.Flags()
	f.StringVar(&o.Set, "set", "latest", "backup label to restore")
	f.StringVar(&o.TargetDir, "target-dir", "", "datadir to restore into (must be empty)")
	f.StringVar(&o.WorkDir, "work-dir", "", "staging directory (default: next to target-dir)")
	f.StringVar(&o.TargetTime, "target-time", "", `point-in-time target "YYYY-MM-DD HH:MM:SS" (server local time)`)
	f.StringVar(&o.PITRDir, "pitr-dir", "", "where binlogs and replay.json are written")
	f.BoolVar(&o.DeleteExisting, "delete-existing", false, "wipe a non-empty target dir first (mysqld must be stopped)")
	f.BoolVar(&o.KeepWork, "keep-work", false, "keep the staging directory")
	_ = cmd.MarkFlagRequired("target-dir")
	return cmd
}

func replayCmd() *cobra.Command {
	var o restore.ReplayOptions
	cmd := &cobra.Command{
		Use:   "replay",
		Short: "Roll a restored, running server forward using fetched binlogs",
		RunE: func(cmd *cobra.Command, _ []string) error {
			e, err := loadEnv(cmd.Context(), false)
			if err != nil {
				return err
			}
			cnf, cleanup, err := mysql.DefaultsFile(e.cfg.MySQL)
			if err != nil {
				return err
			}
			defer cleanup()
			o.MysqlbinlogBin, o.MysqlBin, o.DefaultsFile = e.cfg.Binlog.MysqlbinlogBin, e.cfg.Binlog.MysqlBin, cnf
			return restore.Replay(cmd.Context(), o, g.log)
		},
	}
	cmd.Flags().StringVar(&o.PITRDir, "pitr-dir", "", "directory written by `mqbackrest restore --target-time`")
	cmd.Flags().BoolVar(&o.DryRun, "dry-run", false, "print the pipeline instead of running it")
	_ = cmd.MarkFlagRequired("pitr-dir")
	return cmd
}

type infoOut struct {
	Stanza    string           `json:"stanza"`
	Repo      string           `json:"repo"`
	Encrypted bool             `json:"encrypted"`
	Backups   []*repo.Manifest `json:"backups"`
	Orphans   []string         `json:"incomplete_backups,omitempty"`
	Binlogs   struct {
		Count int      `json:"count"`
		First string   `json:"first,omitempty"`
		Last  string   `json:"last,omitempty"`
		Size  int64    `json:"size"`
		Gaps  []string `json:"gaps,omitempty"`
	} `json:"binlogs"`
}

func infoCmd() *cobra.Command {
	var output string
	cmd := &cobra.Command{
		Use:   "info",
		Short: "Show backups and binlog archive status",
		RunE: func(cmd *cobra.Command, _ []string) error {
			e, err := loadEnv(cmd.Context(), true)
			if err != nil {
				return err
			}
			set, err := e.repo.ListBackups(cmd.Context())
			if err != nil {
				return err
			}
			bls, err := e.repo.ListBinlogs(cmd.Context())
			if err != nil {
				return err
			}
			out := infoOut{Stanza: e.cfg.Stanza, Repo: e.st.String(), Encrypted: e.repo.Encrypted(), Backups: set.Backups, Orphans: set.Orphans}
			out.Binlogs.Count, out.Binlogs.Gaps = len(bls), repo.BinlogGaps(bls)
			if len(bls) > 0 {
				out.Binlogs.First, out.Binlogs.Last = bls[0].Name, bls[len(bls)-1].Name
			}
			for _, b := range bls {
				out.Binlogs.Size += b.Size
			}
			if output == "json" {
				for _, b := range out.Backups { // keep JSON output compact
					b.Parts, b.Info, b.Checkpoints = nil, "", ""
				}
				enc := json.NewEncoder(os.Stdout)
				enc.SetIndent("", "  ")
				return enc.Encode(out)
			}
			printInfo(out)
			return nil
		},
	}
	cmd.Flags().StringVarP(&output, "output", "o", "text", "text or json")
	return cmd
}

func printInfo(o infoOut) {
	fmt.Printf("stanza: %s\n    repo: %s  encrypted: %v\n", o.Stanza, o.Repo, o.Encrypted)
	if len(o.Backups) == 0 {
		fmt.Println("    no backups")
	} else {
		tw := tabwriter.NewWriter(os.Stdout, 2, 4, 2, ' ', 0)
		fmt.Fprintln(tw, "    LABEL\tTYPE\tSTARTED\tDURATION\tSIZE\tSTORED\tLSN\tBINLOG")
		for _, b := range o.Backups {
			bl := "-"
			if b.Binlog != nil {
				bl = fmt.Sprintf("%s:%d", b.Binlog.File, b.Binlog.Position)
			}
			fmt.Fprintf(tw, "    %s\t%s\t%s\t%s\t%s\t%s\t%d-%d\t%s\n", b.Label, b.Type,
				b.StartTime.Local().Format("2006-01-02 15:04:05"), b.StopTime.Sub(b.StartTime).Round(time.Second),
				backup.HumanBytes(b.RawSize), backup.HumanBytes(b.Size), b.FromLSN, b.ToLSN, bl)
		}
		tw.Flush()
	}
	if len(o.Orphans) > 0 {
		fmt.Printf("    incomplete (failed or running): %s\n", strings.Join(o.Orphans, ", "))
	}
	if o.Binlogs.Count == 0 {
		fmt.Println("    binlog archive: empty")
	} else {
		fmt.Printf("    binlog archive: %d files %s .. %s (%s)\n", o.Binlogs.Count, o.Binlogs.First, o.Binlogs.Last, backup.HumanBytes(o.Binlogs.Size))
	}
	if len(o.Binlogs.Gaps) > 0 {
		fmt.Printf("    WARNING binlog gaps: %s\n", strings.Join(o.Binlogs.Gaps, ", "))
	}
}

func verifyCmd() *cobra.Command {
	var label string
	var deep bool
	cmd := &cobra.Command{
		Use:   "verify",
		Short: "Check every stored file against its manifest checksum",
		RunE: func(cmd *cobra.Command, _ []string) error {
			ctx := cmd.Context()
			e, err := loadEnv(ctx, true)
			if err != nil {
				return err
			}
			set, err := e.repo.ListBackups(ctx)
			if err != nil {
				return err
			}
			var problems []string
			for _, b := range set.Backups {
				if label != "" && b.Label != label {
					continue
				}
				if _, err := set.Chain(b.Label); err != nil {
					problems = append(problems, err.Error())
				}
				for _, p := range b.Parts {
					if err := verifyPart(ctx, e.repo, b, p, deep); err != nil {
						problems = append(problems, fmt.Sprintf("%s/%s: %v", b.Label, p.Name, err))
					}
				}
				g.log.Info("verified backup", "label", b.Label, "parts", len(b.Parts))
			}
			if label == "" {
				bls, err := e.repo.ListBinlogs(ctx)
				if err != nil {
					return err
				}
				for _, bl := range bls {
					n, sum, err := e.repo.HashBinlog(ctx, bl)
					if err != nil {
						problems = append(problems, fmt.Sprintf("binlog %s: %v", bl.Name, err))
					} else if n != bl.Size || sum != bl.SHA256 {
						problems = append(problems, fmt.Sprintf("binlog %s: checksum mismatch", bl.Name))
					}
				}
				for _, gap := range repo.BinlogGaps(bls) {
					problems = append(problems, "binlog archive missing "+gap)
				}
				g.log.Info("verified binlogs", "count", len(bls))
			}
			if len(problems) > 0 {
				for _, p := range problems {
					g.log.Error("verify", "problem", p)
				}
				return fmt.Errorf("verify found %d problem(s)", len(problems))
			}
			g.log.Info("verify ok")
			return nil
		},
	}
	cmd.Flags().StringVar(&label, "set", "", "verify only this backup")
	cmd.Flags().BoolVar(&deep, "deep", false, "also decrypt and decompress every file")
	return cmd
}

func verifyPart(ctx context.Context, r *repo.Repo, m *repo.Manifest, p repo.Part, deep bool) error {
	if !deep {
		n, sum, err := r.HashPart(ctx, m, p)
		if err != nil {
			return err
		}
		if n != p.Size || sum != p.SHA256 {
			return errors.New("checksum mismatch")
		}
		return nil
	}
	rc, err := r.GetPart(ctx, m, p)
	if err != nil {
		return err
	}
	n, err := ioCopyDiscard(rc)
	if cerr := rc.Close(); err == nil {
		err = cerr
	}
	if err == nil && n != p.RawSize {
		err = fmt.Errorf("decoded %d bytes, manifest says %d", n, p.RawSize)
	}
	return err
}

func expireCmd() *cobra.Command {
	var dryRun bool
	var keep int
	cmd := &cobra.Command{
		Use:   "expire",
		Short: "Delete backups and binlogs outside the retention policy",
		RunE: func(cmd *cobra.Command, _ []string) error {
			ctx := cmd.Context()
			e, err := loadEnv(ctx, true)
			if err != nil {
				return err
			}
			if keep == 0 {
				keep = e.cfg.Retention.Full
			}
			release, err := lock.Acquire(e.cfg.LockDir, e.cfg.Stanza, "backup")
			if err != nil {
				return err
			}
			defer release()
			set, err := e.repo.ListBackups(ctx)
			if err != nil {
				return err
			}
			bls, err := e.repo.ListBinlogs(ctx)
			if err != nil {
				return err
			}
			plan := repo.PlanExpire(set.Backups, bls, keep)
			verb := "expiring"
			if dryRun {
				verb = "would expire"
			}
			// Incomplete backups are safe to remove: we hold the backup lock.
			for _, l := range append(set.Orphans, plan.Backups...) {
				g.log.Info(verb+" backup", "label", l)
				if !dryRun {
					if err := e.repo.DeleteBackup(ctx, l); err != nil {
						return err
					}
				}
			}
			for _, b := range plan.Binlogs {
				g.log.Info(verb+" binlog", "name", b)
				if !dryRun {
					if err := e.repo.DeleteBinlog(ctx, b); err != nil {
						return err
					}
				}
			}
			g.log.Info("expire done", "backups", len(plan.Backups)+len(set.Orphans), "binlogs", len(plan.Binlogs), "retention_full", keep)
			return nil
		},
	}
	cmd.Flags().BoolVar(&dryRun, "dry-run", false, "show what would be deleted")
	cmd.Flags().IntVar(&keep, "retention-full", 0, "override retention.full")
	return cmd
}

func versionCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "version",
		Short: "Print version",
		Run: func(*cobra.Command, []string) {
			fmt.Printf("mqbackrest %s (commit %s, built %s)\n", version.Version, version.Commit, version.Date)
		},
	}
}
