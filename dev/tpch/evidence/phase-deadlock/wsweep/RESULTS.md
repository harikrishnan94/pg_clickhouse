# Native-vs-offload W-sweep under shared cgroup cpu.max cap

DB=tpch_sf10  N=5 warm runs/cell  W in {8 16}  postmaster=2024103 clickhouse=3250756
cap: one cgroup (/sys/fs/cgroup/pgch_wsweep), both process trees, cpu.max = W*100000us. Cores = CPU-s/wall
(nat_cores = host-CH; off_cons = CH; off_prod = host-CH). speedup = nat_med/off_med.


## Q1  (offload eligibility: 926	59986052)

| W | nat_med_ms | nat_min/max | nat_sd | nat_cores | off_med_ms | off_min/max | off_sd | off_prod | off_cons | speedup |
|--:|-----------:|-------------|-------:|----------:|-----------:|-------------|-------:|---------:|---------:|--------:|
| 8 | 4743 | 4724/4760 | 13 | 8.05 | 1629 | 1626/1633 | 2 | 3.96 | 1.31 | 2.91x |
| 16 | 2510 | 2507/2514 | 3 | 15.95 | 954 | 951/958 | 2 | 7.92 | 2.34 | 2.63x |

## Q5  (offload eligibility: 1200	76586082)

| W | nat_med_ms | nat_min/max | nat_sd | nat_cores | off_med_ms | off_min/max | off_sd | off_prod | off_cons | speedup |
|--:|-----------:|-------------|-------:|----------:|-----------:|-------------|-------:|---------:|---------:|--------:|
| 8 | 1365 | 1356/1397 | 14 | 7.98 | 1534 | 1513/1542 | 10 | 4.35 | 1.30 | 0.89x |
| 16 | 854 | 851/862 | 4 | 15.55 | 967 | 957/1041 | 30 | 8.09 | 2.10 | 0.88x |

## Q10  (offload eligibility: 1195	76486077)

| W | nat_med_ms | nat_min/max | nat_sd | nat_cores | off_med_ms | off_min/max | off_sd | off_prod | off_cons | speedup |
|--:|-----------:|-------------|-------:|----------:|-----------:|-------------|-------:|---------:|---------:|--------:|
| 8 | 6050 | 6017/6058 | 15 | 2.94 | 6280 | 6249/6322 | 30 | 2.02 | 0.60 | 0.96x |
| 16 | 5310 | 5302/5312 | 4 | 3.60 | 5650 | 5604/5709 | 39 | 2.44 | 0.71 | 0.94x |

## Q19  (offload eligibility: 963	61986052)

| W | nat_med_ms | nat_min/max | nat_sd | nat_cores | off_med_ms | off_min/max | off_sd | off_prod | off_cons | speedup |
|--:|-----------:|-------------|-------:|----------:|-----------:|-------------|-------:|---------:|---------:|--------:|
| 8 | 349 | 228/791 | 234 | 5.85 | 2500 | 2241/2680 | 172 | 6.66 | 0.48 | 0.14x |
| 16 | 169 | 166/193 | 12 | 14.24 | 1295 | 1289/1298 | 4 | 8.27 | 1.04 | 0.13x |

