#!/bin/sh
# Wipes the demo cluster's data and starts it fresh. Needed because Quorum has
# no log compaction yet: the whole log stays in memory and is replayed on every
# restart, so without a reset a node would hit its memory limit within a day.
# Run from cron on the VM every 6 hours (installed by deploy.sh).
set -eu
cd "$(dirname "$0")/.."
compose="docker compose -f docker-compose.yml -f docker-compose.prod.yml"
echo "$(date -Is) resetting the demo cluster"
$compose stop loadgen node1 node2 node3
$compose rm -f node1 node2 node3
docker volume rm quorum_data1 quorum_data2 quorum_data3
$compose up -d node1 node2 node3 loadgen
