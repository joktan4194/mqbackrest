// Package config loads and validates the mqBackRest YAML configuration file.
package config

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"regexp"
	"time"

	"gopkg.in/yaml.v3"
)

// Config is the top-level configuration for one stanza (one MySQL cluster).
type Config struct {
	Stanza     string     `yaml:"stanza"`
	Repo       Repo       `yaml:"repo"`
	MySQL      MySQL      `yaml:"mysql"`
	XtraBackup XtraBackup `yaml:"xtrabackup"`
	Binlog     Binlog     `yaml:"binlog"`
	Compress   Compress   `yaml:"compress"`
	Retention  Retention  `yaml:"retention"`
	ProcessMax int        `yaml:"process_max"`
	PartSizeMB int        `yaml:"part_size_mb"`
	LockDir    string     `yaml:"lock_dir"`
}

// Repo describes where backups are stored.
type Repo struct {
	Type       string `yaml:"type"` // "local" or "s3"
	Path       string `yaml:"path"` // local only
	S3         S3     `yaml:"s3"`
	CipherPass string `yaml:"cipher_pass"` // empty disables encryption (only read at stanza-create and when opening)
}

// S3 holds settings for any S3-compatible object store (AWS, MinIO, R2, GCS interop, ...).
type S3 struct {
	Endpoint  string `yaml:"endpoint"`
	Bucket    string `yaml:"bucket"`
	Region    string `yaml:"region"`
	Prefix    string `yaml:"prefix"`
	AccessKey string `yaml:"access_key"`
	SecretKey string `yaml:"secret_key"`
	UseSSL    *bool  `yaml:"use_ssl"`
	PathStyle bool   `yaml:"path_style"`
}

// MySQL holds connection settings for the server being backed up.
type MySQL struct {
	Host     string `yaml:"host"`
	Port     int    `yaml:"port"`
	Socket   string `yaml:"socket"`
	User     string `yaml:"user"`
	Password string `yaml:"password"`
	Datadir  string `yaml:"datadir"`
}

// XtraBackup configures the physical backup engine.
type XtraBackup struct {
	Bin         string   `yaml:"bin"`
	XbstreamBin string   `yaml:"xbstream_bin"`
	Parallel    int      `yaml:"parallel"`
	ExtraArgs   []string `yaml:"extra_args"`   // appended to --backup
	PrepareArgs []string `yaml:"prepare_args"` // appended to --prepare (e.g. --use-memory=2G)
	RestoreArgs []string `yaml:"restore_args"` // appended to --move-back
}

// Binlog configures continuous binlog archiving for point-in-time recovery.
type Binlog struct {
	MysqlbinlogBin string        `yaml:"mysqlbinlog_bin"`
	MysqlBin       string        `yaml:"mysql_bin"`
	SpoolDir       string        `yaml:"spool_dir"`
	ServerID       int           `yaml:"server_id"`
	PollInterval   time.Duration `yaml:"poll_interval"`
}

// Compress configures compression of stored files.
type Compress struct {
	Type  string `yaml:"type"`  // "zstd" or "none"
	Level int    `yaml:"level"` // zstd level 1-22 (mapped onto encoder speed classes)
}

// Retention controls `mqbackrest expire`.
type Retention struct {
	Full int `yaml:"full"` // number of full backups (and their incrementals) to keep
}

var envRef = regexp.MustCompile(`\$\{([A-Za-z_][A-Za-z0-9_]*)\}`)

// expandEnv replaces ${VAR} references only, so literal "$" in passwords is safe.
func expandEnv(b []byte) ([]byte, error) {
	var missing []string
	out := envRef.ReplaceAllFunc(b, func(m []byte) []byte {
		name := string(envRef.FindSubmatch(m)[1])
		v, ok := os.LookupEnv(name)
		if !ok {
			missing = append(missing, name)
		}
		return []byte(v)
	})
	if len(missing) > 0 {
		return nil, fmt.Errorf("config references unset environment variable(s): %v", missing)
	}
	return out, nil
}

// Load reads, expands, decodes, defaults and validates a config file.
func Load(path string) (*Config, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config: %w", err)
	}
	return Parse(raw)
}

// Parse decodes a config from bytes. ${VAR} references are expanded in
// values only (never in comments or keys).
func Parse(raw []byte) (*Config, error) {
	var root yaml.Node
	if err := yaml.Unmarshal(raw, &root); err != nil {
		return nil, fmt.Errorf("parse config: %w", err)
	}
	if err := expandNode(&root); err != nil {
		return nil, err
	}
	expanded, err := yaml.Marshal(&root)
	if err != nil {
		return nil, err
	}
	var c Config
	dec := yaml.NewDecoder(bytes.NewReader(expanded))
	dec.KnownFields(true)
	if err := dec.Decode(&c); err != nil && !errors.Is(err, io.EOF) {
		return nil, fmt.Errorf("parse config: %w", err)
	}
	c.applyDefaults()
	if err := c.Validate(); err != nil {
		return nil, err
	}
	return &c, nil
}

func expandNode(n *yaml.Node) error {
	if n.Kind == yaml.ScalarNode && envRef.MatchString(n.Value) {
		v, err := expandEnv([]byte(n.Value))
		if err != nil {
			return err
		}
		n.Value, n.Tag, n.Style = string(v), "", 0 // re-resolve: ${PORT} may be an int
		return nil
	}
	for i, ch := range n.Content {
		if n.Kind == yaml.MappingNode && i%2 == 0 {
			continue // keys
		}
		if err := expandNode(ch); err != nil {
			return err
		}
	}
	return nil
}

func (c *Config) applyDefaults() {
	if c.Stanza == "" {
		c.Stanza = "main"
	}
	if c.Repo.Type == "" {
		c.Repo.Type = "local"
	}
	if c.Repo.S3.Region == "" {
		c.Repo.S3.Region = "us-east-1"
	}
	if c.MySQL.Host == "" && c.MySQL.Socket == "" {
		c.MySQL.Host = "127.0.0.1"
	}
	if c.MySQL.Port == 0 {
		c.MySQL.Port = 3306
	}
	if c.XtraBackup.Bin == "" {
		c.XtraBackup.Bin = "xtrabackup"
	}
	if c.XtraBackup.XbstreamBin == "" {
		c.XtraBackup.XbstreamBin = "xbstream"
	}
	if c.XtraBackup.Parallel == 0 {
		c.XtraBackup.Parallel = 4
	}
	if c.Binlog.MysqlbinlogBin == "" {
		c.Binlog.MysqlbinlogBin = "mysqlbinlog"
	}
	if c.Binlog.MysqlBin == "" {
		c.Binlog.MysqlBin = "mysql"
	}
	if c.Binlog.SpoolDir == "" {
		c.Binlog.SpoolDir = "/var/lib/mqbackrest/spool"
	}
	if c.Binlog.ServerID == 0 {
		c.Binlog.ServerID = 4242
	}
	if c.Binlog.PollInterval == 0 {
		c.Binlog.PollInterval = 10 * time.Second
	}
	if c.Compress.Type == "" {
		c.Compress.Type = "zstd"
	}
	if c.Compress.Level == 0 {
		c.Compress.Level = 3
	}
	if c.Retention.Full == 0 {
		c.Retention.Full = 2
	}
	if c.ProcessMax == 0 {
		c.ProcessMax = 4
	}
	if c.PartSizeMB == 0 {
		c.PartSizeMB = 32
	}
	if c.LockDir == "" {
		c.LockDir = os.TempDir()
	}
}

// Validate checks the config for obvious mistakes.
func (c *Config) Validate() error {
	var errs []error
	if !regexp.MustCompile(`^[A-Za-z0-9_-]+$`).MatchString(c.Stanza) {
		errs = append(errs, fmt.Errorf("stanza %q must match [A-Za-z0-9_-]+", c.Stanza))
	}
	switch c.Repo.Type {
	case "local":
		if c.Repo.Path == "" {
			errs = append(errs, errors.New("repo.path is required for a local repo"))
		}
	case "s3":
		if c.Repo.S3.Endpoint == "" || c.Repo.S3.Bucket == "" {
			errs = append(errs, errors.New("repo.s3.endpoint and repo.s3.bucket are required for an s3 repo"))
		}
	default:
		errs = append(errs, fmt.Errorf("repo.type %q must be local or s3", c.Repo.Type))
	}
	switch c.Compress.Type {
	case "zstd", "none":
	default:
		errs = append(errs, fmt.Errorf("compress.type %q must be zstd or none", c.Compress.Type))
	}
	if c.Compress.Level < 1 || c.Compress.Level > 22 {
		errs = append(errs, errors.New("compress.level must be 1-22"))
	}
	if c.Retention.Full < 1 {
		errs = append(errs, errors.New("retention.full must be >= 1"))
	}
	if c.ProcessMax < 1 || c.ProcessMax > 64 {
		errs = append(errs, errors.New("process_max must be 1-64"))
	}
	if c.PartSizeMB < 1 || c.PartSizeMB > 1024 {
		errs = append(errs, errors.New("part_size_mb must be 1-1024"))
	}
	return errors.Join(errs...)
}

// SSL reports whether to use TLS for S3 (default true).
func (s S3) SSL() bool { return s.UseSSL == nil || *s.UseSSL }
