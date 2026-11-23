package xtrabackup

import "testing"

func TestParseCheckpoints(t *testing.T) {
	c, err := ParseCheckpoints("backup_type = incremental\nfrom_lsn = 100\nto_lsn = 200\nlast_lsn = 210\nflushed_lsn = 210\n")
	if err != nil || c.FromLSN != 100 || c.ToLSN != 200 || c.LastLSN != 210 || c.BackupType != "incremental" {
		t.Fatalf("%+v %v", c, err)
	}
	if _, err := ParseCheckpoints("backup_type = full\n"); err == nil {
		t.Fatal("expected error")
	}
}

func TestParseInfo(t *testing.T) {
	text := "tool_version = 8.0.35-31\nserver_version = 8.0.40\n" +
		"binlog_pos = filename 'binlog.000007', position '1234', GTID of the last change 'aaaa:1-10,\nbbbb:1-3'\n"
	inf := ParseInfo(text)
	if inf.BinlogFile != "binlog.000007" || inf.BinlogPos != 1234 || inf.GTIDSet != "aaaa:1-10,bbbb:1-3" || inf.ServerVersion != "8.0.40" {
		t.Fatalf("%+v", inf)
	}
	if inf := ParseInfo("binlog_pos = filename 'b.000001', position '4'\n"); inf.BinlogFile != "b.000001" || inf.GTIDSet != "" {
		t.Fatalf("%+v", inf)
	}
}
