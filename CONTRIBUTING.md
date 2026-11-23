# Contributing to mqBackRest

Thanks for helping make MySQL backups boring.

## Ground rules

- Backups are about trust. Every change that touches backup, restore or binlog code needs tests, and `make e2e` must pass.
- Keep the repository format backward compatible. If you must change it, bump `repo.FormatVersion` and keep the old reader working.
- Open an issue before a large change, so the design can be agreed first.

## Dev loop

```bash
make lint test   # fast: unit tests and fake-xtrabackup flow tests
make e2e         # slow: real MySQL + XtraBackup + S3 in Docker (KEEP=1 leaves containers up for debugging)
```

Requirements: Go (see `go.mod`) and Docker with Compose v2.

## Where things live

See [docs/architecture.md](docs/architecture.md). Good first issues are labelled `good first issue`. Roadmap items in the README are open for anyone to pick up.

## Pull requests

- Keep each PR to one logical change, with a clear description of why it's needed.
- Commits should explain the *why*. Squash fixups before merge.
- Run `gofmt`. CI rejects unformatted code.

## Reporting security issues

Please do **not** open a public issue. See [SECURITY.md](SECURITY.md).
