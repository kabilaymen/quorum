#!/bin/sh
# Kills the current leader of the demo cluster, waits, then restarts it, so the
# public dashboard shows real failovers. Run from cron on the VM, e.g.:
#   */10 * * * * /home/aymen/quorum/deploy/chaos.sh >> /home/aymen/chaos.log 2>&1
set -eu
cd "$(dirname "$0")/.."
compose="docker compose -f docker-compose.yml -f docker-compose.prod.yml"

leader=$(curl -fsS --max-time 3 https://quorum.aymenkabil.me/status | jq -r '.leader')
case "$leader" in
  1|2|3) ;;
  *) echo "$(date -Is) no leader known ($leader), skipping"; exit 0 ;;
esac

echo "$(date -Is) killing node$leader"
$compose kill "node$leader"
sleep 20
$compose start "node$leader"
echo "$(date -Is) node$leader restarted"
