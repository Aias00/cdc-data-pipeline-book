#!/bin/sh
set -eu
cd "$(dirname "$0")/.."
: "${CDC_COMPOSE_PROJECT:?Set a dedicated CDC_COMPOSE_PROJECT before running; this lab owns only the dedicated project resources}"
case "$CDC_COMPOSE_PROJECT" in cdc-book-*) ;; *) echo 'Project must start cdc-book-' >&2; exit 2;; esac
mkdir -p "${CDC_EVIDENCE_DIR:-lab/evidence}"
if [ -f "${CDC_EVIDENCE_DIR:-lab/evidence}/kill-ready.json" ]; then
  echo "Refusing stale crash marker; use a fresh evidence directory" >&2
  exit 2
fi
cleanup() {
  docker compose -p "$CDC_COMPOSE_PROJECT" -f lab/compose.yml logs --no-color > "${CDC_EVIDENCE_DIR:-lab/evidence}/containers.log" 2>&1 || true
  docker compose -p "$CDC_COMPOSE_PROJECT" -f lab/compose.yml down --volumes --remove-orphans
}
trap cleanup EXIT
python3 -m pip install -r lab/requirements.txt
docker compose -p "$CDC_COMPOSE_PROJECT" -f lab/compose.yml up -d
for task_service in mysql kafka connect postgres; do
  task_container=$(docker compose -p "$CDC_COMPOSE_PROJECT" -f lab/compose.yml ps -q "$task_service")
  task_image=$(docker inspect --format '{{.Image}}' "$task_container")
  docker image inspect --format '{{json .RepoDigests}}' "$task_image" > "${CDC_EVIDENCE_DIR:-lab/evidence}/image-$task_service.json"
done
docker compose -p "$CDC_COMPOSE_PROJECT" -f lab/compose.yml exec -T kafka /opt/kafka/bin/kafka-topics.sh --version > "${CDC_EVIDENCE_DIR:-lab/evidence}/kafka-version.txt"
python3 lab/run.py
