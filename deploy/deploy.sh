#!/bin/sh
# Starts (or updates) both demos and the Caddy edge on the VM. Idempotent:
# re-run it after a git pull to redeploy.
#   sh ~/quorum/deploy/deploy.sh
set -eu
home=$HOME
# Pull first, then restart on the updated copy of this script: sh reads a
# script as it runs, so the old version would otherwise finish the job.
if [ "${1:-}" != "--pulled" ]; then
  git -C "$home/quorum" pull --ff-only
  exec sh "$home/quorum/deploy/deploy.sh" --pulled
fi
[ -d "$home/vigil" ] || git clone https://github.com/kabilaymen/vigil.git "$home/vigil"
git -C "$home/vigil" pull --ff-only

docker network inspect edge >/dev/null 2>&1 || docker network create edge

# Secrets are generated on the VM, once, and never leave it. Keys added in a
# later version are appended to an existing .env.
ensure() { # file key value
  grep -q "^$2=" "$1" 2>/dev/null || printf '%s=%s\n' "$2" "$3" >> "$1"
}
[ -f "$home/.grafana-admin" ] || { umask 077; openssl rand -base64 18 > "$home/.grafana-admin"; }
for repo in quorum vigil; do
  (umask 077; touch "$home/$repo/.env")
  ensure "$home/$repo/.env" GRAFANA_ADMIN_PASSWORD "$(cat "$home/.grafana-admin")"
done
ensure "$home/vigil/.env" POSTGRES_PASSWORD "$(openssl rand -hex 16)"
ensure "$home/vigil/.env" GRAFANA_DB_PASSWORD "$(openssl rand -hex 16)"

# Images come from GHCR (built by CI). Building on this small VM is only a
# fallback: it competes with the raft nodes for CPU and triggers elections.
for repo in vigil quorum; do
  (
    cd "$home/$repo"
    c="docker compose -f docker-compose.yml -f docker-compose.prod.yml"
    build=""
    $c pull -q || { echo "$repo: pull failed, building on the VM"; build="--build"; }
    $c up -d $build --remove-orphans
    # prometheus.yml is bind-mounted as a single file: git pull replaces the
    # file, and the container keeps reading the old one until it restarts.
    $c restart prometheus >/dev/null
  )
done
(cd "$home/quorum/deploy/edge" && docker compose up -d)

chaos="*/10 * * * * $home/quorum/deploy/chaos.sh >> $home/chaos.log 2>&1"
# The log is compacted (snapshots), so the cluster no longer needs periodic
# resets; deploy/reset-demo.sh stays available for a manual one. Older
# deployments installed it in cron: the grep removes that line.
# "|| true": with no crontab yet, grep matches nothing and set -e would stop
# the block before the new line is written.
{ crontab -l 2>/dev/null | grep -v -e 'deploy/chaos.sh' -e 'deploy/reset-demo.sh' || true; echo "$chaos"; } | crontab -

docker compose ls
echo "Grafana admin password: $home/.grafana-admin"
