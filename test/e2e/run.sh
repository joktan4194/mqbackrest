#!/usr/bin/env bash
# Full backup -> incremental -> binlog archive -> point-in-time restore -> replay,
# against real MySQL 8.0 + Percona XtraBackup + SeaweedFS S3.
set -euo pipefail
cd "$(dirname "$0")"
ROOT=$(cd ../.. && pwd)
ARCH=$(docker version -f '{{.Server.Arch}}')
COMPOSE="docker compose -f docker-compose.yml"

cleanup() { [[ "${KEEP:-}" == 1 ]] || $COMPOSE --profile restored down -v >/dev/null 2>&1 || true; }
trap cleanup EXIT

step() { printf '\n\033[1;34m==> %s\033[0m\n' "$*"; }
sql()  { $COMPOSE exec -T "${2:-mysql}" mysql -uroot -prootpw -N -e "$1" 2>/dev/null; }
mqbackrest() { $COMPOSE exec -T tools /opt/mqbackrest/mqbackrest "$@"; }

step "build mqbackrest for linux/$ARCH"
mkdir -p "$ROOT/dist/e2e"
(cd "$ROOT" && CGO_ENABLED=0 GOOS=linux GOARCH="$ARCH" go build -o dist/e2e/mqbackrest ./cmd/mqbackrest)

step "start mysql, s3, tools"
$COMPOSE --profile restored down -v >/dev/null 2>&1 || true
$COMPOSE up -d --wait mysql s3 tools
for i in $(seq 60); do $COMPOSE exec -T tools curl -s -o /dev/null http://s3:8333/ && break; sleep 1; done

sql "CREATE DATABASE app; CREATE TABLE app.t (id INT AUTO_INCREMENT PRIMARY KEY, phase VARCHAR(16), pad CHAR(200));"
seed() { sql "INSERT INTO app.t (phase, pad) SELECT '$1', REPEAT('x',200) FROM information_schema.columns LIMIT $2;"; }
seed full 2000

step "stanza-create + check"
mqbackrest stanza-create
mqbackrest check

step "full backup"
mqbackrest backup --type full
seed incr 1000

step "incremental backup"
mqbackrest backup --type incr

step "start archive-daemon"
$COMPOSE exec -d tools sh -c '/opt/mqbackrest/mqbackrest archive-daemon > /tmp/archive.log 2>&1'
seed pitr 500
sleep 2
TARGET=$(sql "SELECT NOW();")
sleep 2
seed after 700
sql "FLUSH BINARY LOGS;"
sleep 6   # > poll_interval: let the daemon push the closed binlog
EXPECT=$(sql "SELECT COUNT(*) FROM app.t WHERE phase <> 'after';")
echo "target time: $TARGET   expected rows: $EXPECT"

step "info + verify --deep"
mqbackrest info
mqbackrest verify --deep

step "restore to $TARGET"
mqbackrest restore --target-dir /restore/data --target-time "$TARGET" --pitr-dir /restore/pitr
$COMPOSE exec -T tools chown -R 999:999 /restore/data

step "boot restored server and replay binlogs"
$COMPOSE --profile restored up -d --wait restored
$COMPOSE exec -T -e MYSQL_HOST=restored tools /opt/mqbackrest/mqbackrest replay --pitr-dir /restore/pitr

GOT=$(sql "SELECT COUNT(*) FROM app.t;" restored)
AFTER=$(sql "SELECT COUNT(*) FROM app.t WHERE phase='after';" restored)
echo "restored rows: $GOT (expected $EXPECT), rows written after target: $AFTER (expected 0)"
[[ "$GOT" == "$EXPECT" && "$AFTER" == "0" ]] || { echo "E2E FAILED"; $COMPOSE exec -T tools cat /tmp/archive.log; exit 1; }

step "expire (retention.full=1) after a second full"
mqbackrest backup --type full
mqbackrest expire --dry-run
mqbackrest expire
mqbackrest info

printf '\n\033[1;32mE2E PASSED\033[0m\n'
