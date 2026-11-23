# mqBackRest

**Reliable, parallel backup and point-in-time recovery for MySQL: full and incremental physical backups, continuous binlog archiving, encryption and S3 support in a single binary.**

mqBackRest gives MySQL the kind of backup workflow PostgreSQL users get from tools like pgBackRest:
one binary, one config file, a self-describing repository on S3 or disk, and a restore that is
a single command.

> mqBackRest is an independent project. It is not affiliated with or endorsed by the pgBackRest project.

```text
$ mqbackrest info
stanza: main
    repo: s3://my-mysql-backups/mqbackrest  encrypted: true
    LABEL                              TYPE  STARTED              DURATION  SIZE     STORED   LSN                BINLOG
    20261123-213859F                   full  2026-11-23 21:38:59  3s        77.1MiB  4.2MiB   0-33352271         binlog.000004:197
    20261123-213859F_20261123-213902I  incr  2026-11-23 21:39:02  4s        2.3MiB   71.6KiB  33352271-33644384  binlog.000005:197
    binlog archive: 5 files binlog.000001 .. binlog.000005 (452.0KiB)
```

## Features

- **Full and incremental physical backups.** Uses [Percona XtraBackup](https://www.percona.com/software/mysql-database/percona-xtrabackup) for hot, non-blocking InnoDB copies. Incrementals chain on the previous backup's LSN.
- **Continuous binlog archiving.** `mqbackrest archive-daemon` streams binlogs from the server as a replica would, so there is no cron job and no gap waiting for the next `FLUSH LOGS`.
- **Point-in-time recovery.** `mqbackrest restore --target-time "2026-11-23 14:30:00"` picks the right backup, applies incrementals, fetches the binlogs it needs, and `mqbackrest replay` rolls forward.
- **Parallel compress, encrypt and upload.** The xbstream is split into parts that are compressed, encrypted and uploaded concurrently.
- **zstd compression.**
- **Client-side AES-256-GCM encryption.** Data is encrypted in authenticated, ordered chunks, so tampering and truncation are detected. The repository key is wrapped by a PBKDF2-derived passphrase key.
- **Checksums everywhere.** Every stored file has a SHA-256 recorded in the backup manifest. `mqbackrest verify` checks the repository without needing the key, and `verify --deep` also decrypts and decompresses.
- **Any S3-compatible store** (AWS S3, MinIO, Cloudflare R2, Ceph, SeaweedFS, ...) or a local or NFS path.
- **Retention.** `mqbackrest expire` keeps the last N full backups, their incrementals, and exactly the binlogs they need.
- **Safe by construction.** A manifest is written last, so an interrupted backup is never mistaken for a good one. Locks prevent overlapping runs. Restores refuse to overwrite a non-empty datadir.

## How it compares

|                          | mqBackRest | XtraBackup alone | mysqldump / mydumper | WAL-G (MySQL) |
|--------------------------|:------:|:----------------:|:--------------------:|:-------------:|
| Hot physical backup      | ✅ | ✅ | ❌ logical | ✅ |
| Incremental chains       | ✅ | manual | ❌ | ✅ |
| Continuous binlog archive| ✅ daemon | ❌ | ❌ | ✅ |
| PITR in one command      | ✅ | ❌ | ❌ | ✅ |
| Repository + retention   | ✅ | ❌ | ❌ | ✅ |
| Integrity verification   | ✅ | ❌ | ❌ | partial |
| Single config file       | ✅ | — | — | env vars |

## Quick start

### 1. Requirements

- MySQL 8.0 or Percona Server 8.0 with `log_bin=ON` and `binlog_format=ROW`. `gtid_mode=ON` is recommended.
- `xtrabackup` and `xbstream` matching your server's major and minor version, plus `mysqlbinlog` and `mysql`.
- mqBackRest runs on the database host, because XtraBackup needs to read the datadir.

### 2. Create a backup user

```sql
CREATE USER 'mqbackrest'@'localhost' IDENTIFIED BY '...';
GRANT BACKUP_ADMIN, PROCESS, RELOAD, LOCK TABLES, REPLICATION CLIENT, REPLICATION SLAVE ON *.* TO 'mqbackrest'@'localhost';
GRANT SELECT ON performance_schema.log_status TO 'mqbackrest'@'localhost';
GRANT SELECT ON performance_schema.keyring_component_status TO 'mqbackrest'@'localhost';
```

### 3. Install and configure

```bash
curl -L https://github.com/joktan4194/mqbackrest/releases/latest/download/mqbackrest_linux_amd64.tar.gz | tar xz
sudo install mqbackrest /usr/bin/
sudo mkdir -p /etc/mqbackrest && sudo cp mqbackrest.example.yaml /etc/mqbackrest/mqbackrest.yaml   # then edit it
```

### 4. Back up

```bash
mqbackrest stanza-create        # once: initialises the repository (and its encryption key)
mqbackrest check                # validates MySQL settings, tools and repo access
mqbackrest backup --type full
mqbackrest backup               # incremental on top of the latest backup
systemctl enable --now mqbackrest-archive.service       # continuous binlog archiving
systemctl enable --now mqbackrest-backup-full.timer mqbackrest-backup-incr.timer
```

### 5. Restore

```bash
systemctl stop mysqld
mqbackrest restore --target-dir /var/lib/mysql --delete-existing                                  # latest backup
mqbackrest restore --target-dir /var/lib/mysql --delete-existing --target-time "2026-11-23 14:30:00"  # PITR
chown -R mysql:mysql /var/lib/mysql && systemctl start mysqld
mqbackrest replay --pitr-dir /var/lib/mqbackrest-pitr-<label>   # PITR only: printed by restore
```

> **Test your restores.** A backup you have never restored is a hope, not a backup.
> `make e2e` shows the whole cycle working in Docker.

## Commands

| Command | What it does |
|---|---|
| `stanza-create` | Initialise the repository for a cluster (compression and encryption are fixed here). |
| `check` | Verify MySQL configuration, tool versions and repository access. |
| `backup [--type full\|incr]` | Take a backup. `incr` falls back to `full` when no backup exists. |
| `archive-daemon` | Stream and archive binlogs continuously. Run it under systemd. |
| `restore` | Restore the latest backup, `--set <label>`, or `--target-time` into an empty datadir. |
| `replay` | Apply fetched binlogs to a restored, running server up to the target time. |
| `info [-o json]` | List backups, sizes, LSN ranges, binlog positions and archive coverage. |
| `verify [--deep] [--set]` | Check every stored file against its checksum and look for binlog gaps. |
| `expire [--dry-run]` | Apply the retention policy. |

## How it works

See [docs/architecture.md](docs/architecture.md) for the repository layout, the encryption format and the backup and restore pipelines.

## Development

```bash
make lint test   # unit + flow tests (no MySQL needed: fakes stand in for xtrabackup)
make e2e         # real MySQL 8.0 + XtraBackup + S3 in Docker: backup, incr, PITR restore, replay, expire
```

Contributions are welcome. Start with [CONTRIBUTING.md](CONTRIBUTING.md).

## Roadmap

- [ ] Delta restore (only copy files whose checksum changed)
- [ ] Multiple repositories (for example, local plus S3) with independent retention
- [ ] Backup from a replica, with automatic source binlog coordinates
- [ ] Azure Blob and native GCS backends
- [ ] MariaDB (`mariabackup`) and MySQL 8.4 LTS support matrix
- [ ] Prometheus metrics and `info` freshness alerts
- [ ] Resumable uploads for very large backups
- [ ] Restore-test command that boots a scratch mysqld and runs checks

## License

[MIT](LICENSE)
