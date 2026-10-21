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

# One Grafana admin password, generated once and kept only on the VM.
for repo in quorum vigil; do
  if [ ! -f "$home/$repo/.env" ]; then
    [ -f "$home/.grafana-admin" ] || { umask 077; openssl rand -base64 18 > "$home/.grafana-admin"; }
    printf 'GRAFANA_ADMIN_PASSWORD=%s\n' "$(cat "$home/.grafana-admin")" > "$home/$repo/.env"
    chmod 600 "$home/$repo/.env"
  fi
done

for repo in vigil quorum; do
  (cd "$home/$repo" && docker compose -f docker-compose.yml -f docker-compose.prod.yml up -d --build --remove-orphans)
done
(cd "$home/quorum/deploy/edge" && docker compose up -d)

chaos="*/10 * * * * $home/quorum/deploy/chaos.sh >> $home/chaos.log 2>&1"
# The reset runs at minute 5, between two chaos rounds.
reset="5 */6 * * * $home/quorum/deploy/reset-demo.sh >> $home/chaos.log 2>&1"
# "|| true": with no crontab yet, grep matches nothing and set -e would stop
# the block before the new lines are written.
{ crontab -l 2>/dev/null | grep -v -e 'deploy/chaos.sh' -e 'deploy/reset-demo.sh' || true; echo "$chaos"; echo "$reset"; } | crontab -

docker compose ls
echo "Grafana admin password: $home/.grafana-admin"
