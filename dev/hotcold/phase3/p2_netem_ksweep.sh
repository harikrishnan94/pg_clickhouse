#!/usr/bin/env bash
# Hot-Cold Phase 3 P2 it2/it3 — netem-emulated NIC regime K-sweep (the BINDING overlap proof, H11/H12).
# Installs a netem qdisc on lo (delay + rate) and shrinks the PRODUCER's PER-SOCKET SO_SNDBUF toward the
# link BDP via the pg_clickhouse.tcp_sndbuf_bytes GUC (NOT net.core.wmem_max — clamping that global sysctl
# small starves netlink/`tc`, see L0026). With SO_SNDBUF << one frame, K=1 stalls per frame and K-in-flight
# (the run-ahead pool) gates throughput. ALWAYS removes the qdisc in a trap (H12); leaves wmem_max alone.
#   DELAY=50us RATE=3gbit SNDBUF=131072 TAG=netem3g METHOD=epoll ROUNDS=2 KLIST="1 2 4 8" \
#       bash dev/hotcold/phase3/p2_netem_ksweep.sh
set -uo pipefail
DELAY="${DELAY:-50us}"; RATE="${RATE:-3gbit}"; SNDBUF="${SNDBUF:-131072}"
TAG="${TAG:-netem3g}"; METHOD="${METHOD:-epoll}"; ROUNDS="${ROUNDS:-2}"; KLIST="${KLIST:-1 2 4 8}"; N="${N:-5}"
HERE=/home/ubuntu/pg_clickhouse

restore(){
  sudo tc qdisc del dev lo root 2>/dev/null || true
  echo "RESTORED: lo qdisc='$(tc qdisc show dev lo | head -1)'"
}
trap restore EXIT
sudo tc qdisc del dev lo root 2>/dev/null || true
sudo tc qdisc add dev lo root netem delay "$DELAY" rate "$RATE"
echo "=== netem TAG=$TAG: $(tc qdisc show dev lo | head -1) ; producer SO_SNDBUF=$SNDBUF (per-socket GUC); "
echo "    wmem_max=$(sysctl -n net.core.wmem_max) (left at default — netlink-safe). BDP≈ rate*delay ==="
SNDBUF="$SNDBUF" REGIME="$TAG" METHOD="$METHOD" ROUNDS="$ROUNDS" KLIST="$KLIST" N="$N" \
    CBQ="${CBQ:-17}" TPQ="${TPQ:-6 9}" bash "$HERE/dev/hotcold/phase3/p2_ksweep.sh"
echo "=== P2 NETEM KSWEEP DONE TAG=$TAG  $(date -u +%FT%TZ) ==="
# restore() runs on EXIT
