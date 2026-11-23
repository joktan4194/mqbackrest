package cli

import (
	"bytes"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/spf13/cobra"

	"github.com/joktan4194/mqbackrest/internal/mysql"
	"github.com/joktan4194/mqbackrest/internal/repo"
)

func ioCopyDiscard(r io.Reader) (int64, error) { return io.Copy(io.Discard, r) }

func checkCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "check",
		Short: "Validate MySQL settings, tools and repository access",
		RunE: func(cmd *cobra.Command, _ []string) error {
			ctx := cmd.Context()
			e, err := loadEnv(ctx, false)
			if err != nil {
				return err
			}
			failed := 0
			report := func(ok bool, what, detail string) {
				mark := "ok  "
				if !ok {
					mark, failed = "FAIL", failed+1
				}
				fmt.Printf("[%s] %-28s %s\n", mark, what, detail)
			}
			warn := func(what, detail string) { fmt.Printf("[warn] %-28s %s\n", what, detail) }

			// Repository round trip.
			key := e.cfg.Stanza + "/.mqbackrest-check-" + fmt.Sprint(time.Now().UnixNano())
			if err := e.st.Put(ctx, key, bytes.NewReader([]byte("ok"))); err != nil {
				report(false, "repo writable", err.Error())
			} else {
				_ = e.st.Delete(ctx, key)
				report(true, "repo writable", e.st.String())
			}
			if r, err := repo.Open(ctx, e.st, e.cfg); err != nil {
				report(false, "stanza", err.Error())
			} else {
				report(true, "stanza", fmt.Sprintf("%s (compress=%s encrypted=%v)", r.Stanza.Name, r.Stanza.Compress, r.Encrypted()))
			}

			xb, cleanup, _ := e.runner(false)
			defer cleanup()
			if v, err := xb.Version(ctx); err != nil {
				report(false, "xtrabackup", err.Error())
			} else {
				report(true, "xtrabackup", v)
			}

			db, err := mysql.Connect(e.cfg.MySQL)
			if err == nil {
				err = db.PingContext(ctx)
			}
			if err != nil {
				report(false, "mysql connection", err.Error())
			} else {
				defer db.Close()
				vars, err := mysql.Variables(ctx, db, "version", "log_bin", "binlog_format", "gtid_mode", "binlog_row_image", "binlog_expire_logs_seconds", "datadir")
				if err != nil {
					report(false, "mysql variables", err.Error())
				} else {
					report(true, "mysql connection", "server "+vars["version"])
					report(vars["log_bin"] == "ON", "log_bin", vars["log_bin"]+" (required for point-in-time recovery)")
					report(strings.EqualFold(vars["binlog_format"], "ROW"), "binlog_format", vars["binlog_format"]+" (ROW required)")
					if vars["gtid_mode"] != "ON" {
						warn("gtid_mode", vars["gtid_mode"]+" (ON recommended)")
					}
					if vars["binlog_row_image"] != "" && vars["binlog_row_image"] != "FULL" {
						warn("binlog_row_image", vars["binlog_row_image"]+" (FULL recommended)")
					}
					if e.cfg.MySQL.Datadir != "" && vars["datadir"] != "" &&
						strings.TrimRight(vars["datadir"], "/") != strings.TrimRight(e.cfg.MySQL.Datadir, "/") {
						warn("datadir", fmt.Sprintf("config %s, server reports %s", e.cfg.MySQL.Datadir, vars["datadir"]))
					}
				}
			}
			if failed > 0 {
				return fmt.Errorf("%d check(s) failed", failed)
			}
			return nil
		},
	}
}
