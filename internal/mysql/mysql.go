// Package mysql handles credentials and server introspection.
package mysql

import (
	"context"
	"database/sql"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	driver "github.com/go-sql-driver/mysql"

	"github.com/joktan4194/mqbackrest/internal/config"
)

// DefaultsFile writes a 0600 option file so passwords never appear in
// process arguments. The returned cleanup removes it.
func DefaultsFile(c config.MySQL) (string, func(), error) {
	dir, err := os.MkdirTemp("", "mqbackrest-cnf-")
	if err != nil {
		return "", nil, err
	}
	cleanup := func() { os.RemoveAll(dir) }
	var b strings.Builder
	for _, group := range []string{"client", "xtrabackup"} {
		fmt.Fprintf(&b, "[%s]\n", group)
		if c.User != "" {
			fmt.Fprintf(&b, "user=%s\n", quote(c.User))
		}
		if c.Password != "" {
			fmt.Fprintf(&b, "password=%s\n", quote(c.Password))
		}
		if c.Socket != "" {
			fmt.Fprintf(&b, "socket=%s\n", quote(c.Socket))
		} else {
			fmt.Fprintf(&b, "host=%s\nport=%d\n", quote(c.Host), c.Port)
		}
		b.WriteString("\n")
	}
	p := filepath.Join(dir, "mqbackrest.cnf")
	if err := os.WriteFile(p, []byte(b.String()), 0o600); err != nil {
		cleanup()
		return "", nil, err
	}
	return p, cleanup, nil
}

func quote(s string) string {
	r := strings.NewReplacer(`\`, `\\`, `"`, `\"`)
	return `"` + r.Replace(s) + `"`
}

// Connect opens a database handle to the configured server.
func Connect(c config.MySQL) (*sql.DB, error) {
	dc := driver.NewConfig()
	dc.User, dc.Passwd = c.User, c.Password
	if c.Socket != "" {
		dc.Net, dc.Addr = "unix", c.Socket
	} else {
		dc.Net, dc.Addr = "tcp", net.JoinHostPort(c.Host, strconv.Itoa(c.Port))
	}
	dc.AllowNativePasswords = true
	dc.InterpolateParams = true // SHOW ... LIKE ? cannot be server-prepared
	return sql.Open("mysql", dc.FormatDSN())
}

// Variables fetches global variables by name.
func Variables(ctx context.Context, db *sql.DB, names ...string) (map[string]string, error) {
	out := map[string]string{}
	for _, n := range names {
		var name, val string
		err := db.QueryRowContext(ctx, "SHOW GLOBAL VARIABLES LIKE ?", n).Scan(&name, &val)
		if err == sql.ErrNoRows {
			continue
		}
		if err != nil {
			return nil, err
		}
		out[n] = val
	}
	return out, nil
}

// BinaryLogs lists binlog files currently present on the server, oldest first.
func BinaryLogs(ctx context.Context, db *sql.DB) ([]string, error) {
	rows, err := db.QueryContext(ctx, "SHOW BINARY LOGS")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	cols, err := rows.Columns()
	if err != nil {
		return nil, err
	}
	var out []string
	for rows.Next() {
		vals := make([]sql.RawBytes, len(cols))
		ptrs := make([]any, len(cols))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			return nil, err
		}
		out = append(out, string(vals[0]))
	}
	return out, rows.Err()
}
