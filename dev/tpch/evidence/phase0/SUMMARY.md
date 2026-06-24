| Q | verdict | shmscan | strmd_q | ShmBlocks | read_rows | pg_join | pg_agg | ch_join | ch_grpby | rows off/on | fidelity |
|---|---------|--------:|--------:|----------:|----------:|--------:|-------:|:-------:|:--------:|------------|----------|
| 1 | none | 0 | 0 | 0 | 0 | 0 | 2 | no | no | 4/4 | exact |
| 2 | scan_only | 2 | 1 | 2 | 5 | 0 | 1 | yes | no | 100/0 | DIFF(100 lines) |
| 3 | scan_only | 1 | 1 | 1195 | 76486052 | 0 | 1 | yes | no | 10/0 | DIFF(10 lines) |
| 4 | fully | 1 | 1 | 1166 | 74986052 | 0 | 0 | yes | yes | 5/5 | exact |
| 5 | scan_only | 1 | 1 | 2 | 5 | 0 | 1 | yes | no | 5/0 | DIFF(5 lines) |
| 6 | scan_only | 1 | 1 | 928 | 59986052 | 0 | 1 | no | no | 1/1 | exact |
| 7 | scan_only | 1 | 1 | 2 | 25 | 0 | 1 | yes | no | 4/0 | DIFF(4 lines) |
| 8 | scan_only | 1 | 1 | 269 | 16500005 | 0 | 1 | yes | no | 2/0 | DIFF(2 lines) |
| 9 | scan_only? | 1 | 0 | 0 | 0 | 0 | 1 | no | no | 175/0 | ON_ERR |
| 10 | scan_only | 1 | 1 | 1196 | 76486077 | 0 | 1 | yes | no | 381105/381105 | exact |
| 11 | scan_only | 2 | 1 | 2 | 25 | 0 | 2 | yes | no | 8685/0 | DIFF(8685 lines) |
| 12 | fully | 1 | 1 | 926 | 59986052 | 0 | 0 | yes | yes | 2/0 | DIFF(2 lines) |
| 13 | scan_only | 1 | 1 | 268 | 16500000 | 0 | 1 | yes | yes | 46/46 | exact |
| 14 | scan_only? | 1 | 0 | 0 | 0 | 0 | 1 | no | no | 1/0 | ON_ERR |
| 15 | scan_only | 3 | 3 | 1861 | 120072104 | 1 | 3 | no | no | 1/1 | exact |
| 16 | scan_only? | 3 | 0 | 0 | 0 | 1 | 1 | no | no | 27840/0 | ON_ERR |
| 17 | scan_only | 2 | 1 | 36 | 2000000 | 0 | 2 | yes | no | 1/1 | DIFF(2 lines) |
| 18 | scan_only | 1 | 1 | 926 | 59986052 | 3 | 1 | no | yes | 624/624 | exact |
| 19 | scan_only | 1 | 1 | 36 | 2000000 | 0 | 1 | yes | no | 1/1 | DIFF(2 lines) |
| 20 | scan_only | 4 | 1 | 2 | 25 | 1 | 1 | yes | no | 1804/0 | DIFF(1804 lines) |
| 21 | scan_only? | 2 | 0 | 0 | 0 | 0 | 1 | no | no | 100/0 | ON_ERR |
| 22 | scan_only? | 3 | 0 | 0 | 0 | 0 | 2 | no | no | 7/0 | ON_ERR |
