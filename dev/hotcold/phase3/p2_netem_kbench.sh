#!/usr/bin/env bash
# netem wrapper for the lean P2 K-bench (H11/H12). Installs netem (delay+rate) on lo, runs p2_kbench.sh with
# a per-socket SO_SNDBUF (GUC, netlink-safe — NOT global wmem_max), and ALWAYS removes the qdisc in a trap.
#   DELAY=50us RATE=3gbit SNDBUF=65536 TAG=netem3gzc METHOD=msg_zerocopy ROUNDS=3 N=4 QFILES="6 9" \
#       bash dev/hotcold/phase3/p2_netem_kbench.sh
set -uo pipefail
DELAY="${DELAY:-50us}"; RATE="${RATE:-3gbit}"; SNDBUF="${SNDBUF:-65536}"
TAG="${TAG:-netem3gzc}"; METHOD="${METHOD:-msg_zerocopy}"; ROUNDS="${ROUNDS:-3}"; N="${N:-4}"; QFILES="${QFILES:-6 9}"
HERE=/home/ubuntu/pg_clickhouse
restore(){ sudo tc qdisc del dev lo root 2>/dev/null || true; echo "RESTORED lo='$(tc qdisc show dev lo|head -1)'"; }
trap restore EXIT
sudo tc qdisc del dev lo root 2>/dev/null || true
sudo tc qdisc add dev lo root netem delay "$DELAY" rate "$RATE"
echo "=== netem TAG=$TAG: $(tc qdisc show dev lo|head -1); producer SO_SNDBUF=$SNDBUF (per-socket); wmem_max=$(sysctl -n net.core.wmem_max) ==="
TAG="$TAG" METHOD="$METHOD" KLIST="${KLIST:-1 2 4 8}" ROUNDS="$ROUNDS" N="$N" SNDBUF="$SNDBUF" QFILES="$QFILES" \
    bash "$HERE/dev/hotcold/phase3/p2_kbench.sh"
echo "=== P2 NETEM KBENCH DONE TAG=$TAG $(date -u +%FT%TZ) ==="
