# ClickBench native-vs-offload W-sweep under shared cgroup cpu.max cap

DB=clickbench  N=5 warm runs/cell  W in {8}  postmaster=2024103 clickhouse=2673848  MT=16
cap: one cgroup (/sys/fs/cgroup/pgch_cbwsweep), both process trees, cpu.max = W*100000us. Cores = CPU-s/wall
(nat_cores = host-CH; off_cons = CH; off_prod = host-CH). speedup = nat_med/off_med.


## Q4  (offload eligibility: 164	10000000)

| W | nat_med_ms | nat_min/max | nat_sd | nat_cores | off_med_ms | off_min/max | off_sd | off_prod | off_cons | speedup |
|--:|-----------:|-------------|-------:|----------:|-----------:|-------------|-------:|---------:|---------:|--------:|
| 8 | 486 | 482/488 | 2 | 7.71 | 376 | 373/382 | 3 | 7.40 | 0.11 | 1.29x |

