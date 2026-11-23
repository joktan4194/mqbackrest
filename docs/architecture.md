# Architecture

mqBackRest is an orchestrator. It does not reimplement InnoDB's hot-copy logic. Percona XtraBackup already does that well, so mqBackRest adds what XtraBackup lacks: a repository, parallel transport, encryption, integrity, binlog archiving, retention and a one-command restore.

## Code map

| Package | Responsibility |
|---|---|
| `cmd/mqbackrest` | Entry point |
| `internal/cli` | Cobra commands, flag parsing, wiring |
| `internal/config` | YAML config, `${ENV}` expansion, defaults, validation |
| `internal/storage` | `Storage` interface; `local` (atomic rename) and `s3` (minio-go) backends |
| `internal/pipeline` | Encode (zstd, then encrypt, then SHA-256) and decode |
| `internal/encrypt` | Streaming AES-256-GCM format and the master key envelope |
| `internal/repo` | Repository layout, stanza, manifests, chains, binlog archive, retention planning |
| `internal/xtrabackup` | Runs `xtrabackup` and `xbstream`, parses checkpoints and info |
| `internal/backup` | Backup orchestration: stream, split, parallel upload, manifest |
| `internal/restore` | Restore orchestration: chain, download, prepare, PITR fetch, move-back, replay |
| `internal/binlog` | `archive-daemon`: mirror binlogs with `mysqlbinlog --raw --stop-never` |
| `internal/mysql` | Option-file credentials, server introspection |
| `internal/lock` | Host-local `flock` locks |

## Repository layout

```
<prefix>/<stanza>/stanza.json                          format version, compression, wrapped master key
<prefix>/<stanza>/backup/<label>/manifest.json[.enc]   written LAST; its presence = backup is complete
<prefix>/<stanza>/backup/<label>/data/000000.xbs.zst.enc
<prefix>/<stanza>/backup/<label>/data/000001.xbs.zst.enc
<prefix>/<stanza>/binlog/binlog.000042.zst.enc         archived binlog
<prefix>/<stanza>/binlog/binlog.000042.json.enc        sidecar: sizes + SHA-256
```

Labels sort by time and encode their chain:

- full: `20261123-020000F`
- incremental: `20261123-020000F_20261124-020000I`, where the prefix names the full backup it depends on

A backup directory without a manifest is an **orphan** (it failed, or is still running). `expire` removes orphans while holding the backup lock.

## Backup pipeline

```
xtrabackup --backup --stream=xbstream [--incremental-lsn=<prior.to_lsn>]
     │ stdout
     ▼
 splitter ── fixed-size parts (part_size_mb) ──► N workers (process_max)
                                                   zstd → AES-GCM → SHA-256 → Storage.Put
     ▼
 xtrabackup_checkpoints + xtrabackup_info (via --extra-lsndir)
     ▼
 manifest.json  (LSNs, binlog file:pos, GTID set, part list with checksums)
```

Memory is bounded at `(process_max + 1) * part_size_mb`. If any step fails, the whole run is cancelled, xtrabackup is killed, and the partial backup is deleted.

## Restore pipeline

1. Pick the backup: `--set`, `latest`, or for PITR the newest backup that finished before `--target-time`.
2. Resolve the chain (full first, then each incremental).
3. For each backup, stream its parts in order through decrypt and decompress into `xbstream -x`. The checksum of each part is checked as it streams.
4. `xtrabackup --prepare --apply-log-only` on the base, merge each incremental, and run the final prepare without `--apply-log-only`.
5. For PITR: download binlogs from the backup's binlog file onward, stop at any gap, and write `replay.json`.
6. `xtrabackup --move-back` into the (empty) datadir.
7. After mysqld starts: `mqbackrest replay` runs `mysqlbinlog --start-position=<pos> --stop-datetime=<t> files… | mysql`.

## Binlog archiving

`archive-daemon` runs `mysqlbinlog --read-from-remote-server --raw --stop-never` into a spool directory, which mirrors the server's binlogs in real time. Every spool file except the newest is complete. Those are pushed to the repository and then deleted locally. On restart it resumes from the partial file in the spool, or from the file after the last archived one. If the server has already purged a needed file, the daemon logs a **GAP** error, and `info` and `verify` report it.

## Encryption format

```
"MQBRENC1" | 8-byte random nonce prefix | chunk*
chunk = uint32 header (bit 31 = final, bits 0-30 = ciphertext length) | AES-256-GCM ciphertext
nonce = prefix || uint32 counter      AAD = final flag
```

- Chunks hold up to 64 KiB of plaintext. The counter binds their order, and the final flag makes truncation detectable.
- Every file uses a fresh random prefix.
- The 256-bit master key is random. It is wrapped with a key derived by PBKDF2-SHA256 (600k iterations) from `repo.cipher_pass` and stored in `stanza.json`.
- File checksums cover the **stored** bytes, so `verify` works without the passphrase.

## Design rules for contributors

- **Never report success without proof.** The manifest is written last, every read is checksummed, and gaps are surfaced loudly.
- **Never destroy data implicitly.** Restores require an empty datadir or an explicit `--delete-existing`.
- **Never put secrets in process arguments.** Credentials go through a 0600 `--defaults-extra-file`.
- **The repository format is a contract.** Changing it means bumping `repo.FormatVersion` and keeping read support for older formats.
