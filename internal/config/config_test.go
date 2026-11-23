package config

import (
	"strings"
	"testing"
)

func TestParseDefaultsAndEnv(t *testing.T) {
	t.Setenv("PASS", "s3cr$t")
	c, err := Parse([]byte("stanza: prod\nrepo:\n  path: /backups\n  cipher_pass: ${PASS}\nmysql:\n  password: lit$eral\n"))
	if err != nil {
		t.Fatal(err)
	}
	if c.Repo.CipherPass != "s3cr$t" || c.MySQL.Password != "lit$eral" {
		t.Fatalf("env expansion wrong: %q %q", c.Repo.CipherPass, c.MySQL.Password)
	}
	if c.Repo.Type != "local" || c.Compress.Type != "zstd" || c.Retention.Full != 2 || c.MySQL.Port != 3306 {
		t.Fatalf("defaults not applied: %+v", c)
	}
}

func TestEnvTypes(t *testing.T) {
	t.Setenv("PORT", "3307")
	t.Setenv("PW", "123")
	t.Setenv("WEIRD", "a: b # c 'd\"")
	c, err := Parse([]byte("repo:\n  path: /x\n  cipher_pass: ${WEIRD}\nmysql:\n  port: ${PORT}\n  password: ${PW}\n"))
	if err != nil {
		t.Fatal(err)
	}
	if c.MySQL.Port != 3307 || c.MySQL.Password != "123" || c.Repo.CipherPass != "a: b # c 'd\"" {
		t.Fatalf("got port=%d pw=%q cipher=%q", c.MySQL.Port, c.MySQL.Password, c.Repo.CipherPass)
	}
}

func TestParseErrors(t *testing.T) {
	cases := map[string]string{
		"unknown field":   "repo:\n  path: /x\n  pathh: /y\n",
		"missing env":     "repo:\n  path: ${MQBACKREST_SURELY_UNSET_VAR}\n",
		"bad stanza":      "stanza: 'a b'\nrepo:\n  path: /x\n",
		"s3 needs bucket": "repo:\n  type: s3\n  s3:\n    endpoint: x\n",
	}
	for name, y := range cases {
		if _, err := Parse([]byte(y)); err == nil {
			t.Errorf("%s: expected error", name)
		} else if testing.Verbose() {
			t.Logf("%s: %v", name, strings.ReplaceAll(err.Error(), "\n", "; "))
		}
	}
}
