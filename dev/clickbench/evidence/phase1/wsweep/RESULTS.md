# ClickBench native-vs-offload W-sweep under shared cgroup cpu.max cap

DB=clickbench  N=5 warm runs/cell  W in {8}  postmaster=2024103 clickhouse=2394956  MT=16
cap: one cgroup (/sys/fs/cgroup/pgch_cbwsweep), both process trees, cpu.max = W*100000us. Cores = CPU-s/wall
(nat_cores = host-CH; off_cons = CH; off_prod = host-CH). speedup = nat_med/off_med.


## Q19  (offload eligibility: 163	10000000)

| W | nat_med_ms | nat_min/max | nat_sd | nat_cores | off_med_ms | off_min/max | off_sd | off_prod | off_cons | speedup |
|--:|-----------:|-------------|-------:|----------:|-----------:|-------------|-------:|---------:|---------:|--------:|
| 8 | 3791 | 3777/3804 | 11 | 3.07 | 2504 | 2502/2536 | 13 | 2.16 | 0.67 | 1.51x |

## Q25  (offload eligibility: 166	10000000)

| W | nat_med_ms | nat_min/max | nat_sd | nat_cores | off_med_ms | off_min/max | off_sd | off_prod | off_cons | speedup |
|--:|-----------:|-------------|-------:|----------:|-----------:|-------------|-------:|---------:|---------:|--------:|
| 8 | 631 | 622/638 | 6 | 7.04 | 1112 | 1111/1127 | 7 | 3.65 | 0.22 | 0.57x |

## Q43  (offload eligibility: 162	10000000)

| W | nat_med_ms | nat_min/max | nat_sd | nat_cores | off_med_ms | off_min/max | off_sd | off_prod | off_cons | speedup |
|--:|-----------:|-------------|-------:|----------:|-----------:|-------------|-------:|---------:|---------:|--------:|
| 8 | 678 | 672/684 | 5 | 6.80 | 414 | 411/416 | 2 | 7.41 | 0.22 | 1.64x |

