# Native-vs-offload W-sweep under shared cgroup cpu.max cap

DB=tpch_sf10  N=5 warm runs/cell  W in {8 16}  postmaster=2024103 clickhouse=3250756
cap: one cgroup (/sys/fs/cgroup/pgch_wsweep), both process trees, cpu.max = W*100000us. Cores = CPU-s/wall
(nat_cores = host-CH; off_cons = CH; off_prod = host-CH). speedup = nat_med/off_med.


## Q8  (offload eligibility: 1237	78586107)

| W | nat_med_ms | nat_min/max | nat_sd | nat_cores | off_med_ms | off_min/max | off_sd | off_prod | off_cons | speedup |
|--:|-----------:|-------------|-------:|----------:|-----------:|-------------|-------:|---------:|---------:|--------:|
| 8 | 752 | 745/772 | 9 | 7.79 | 2486 | 2469/2529 | 20 | 2.93 | 4.95 | 0.30x |
| 16 | 500 | 498/504 | 2 | 14.33 | 74 | 69/75 | 3 | 7.86 | 0.00 | 6.77x |

## Q9  (offload eligibility: NO(no-block))

| W | nat_med_ms | nat_min/max | nat_sd | nat_cores | off_med_ms | off_min/max | off_sd | off_prod | off_cons | speedup |
|--:|-----------:|-------------|-------:|----------:|-----------:|-------------|-------:|---------:|---------:|--------:|
| 8 | 9452 | 9448/9493 | 19 | 3.84 | 2881 | 2873/2910 | 13 | 2.83 | 5.22 | 3.28x |
| 16 | 8072 | 8061/8105 | 16 | 5.39 | 1700 | 1633/1760 | 41 | 5.68 | 9.43 | 4.75x |

## Q14  (offload eligibility: 961	61986052)

| W | nat_med_ms | nat_min/max | nat_sd | nat_cores | off_med_ms | off_min/max | off_sd | off_prod | off_cons | speedup |
|--:|-----------:|-------------|-------:|----------:|-----------:|-------------|-------:|---------:|---------:|--------:|
| 8 | 9109 | 9020/9150 | 46 | 8.07 | 1338 | 1329/1350 | 7 | 4.17 | 0.40 | 6.81x |
| 16 | 5140 | 5109/5202 | 33 | 16.04 | 807 | 805/811 | 2 | 8.10 | 0.76 | 6.37x |

