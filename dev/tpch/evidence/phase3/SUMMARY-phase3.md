| Q | verdict | shmscan | strmd_q | ShmBlocks | read_rows | pg_join | pg_agg | ch_join | ch_grpby | rows off/on | fidelity |
|---|---------|--------:|--------:|----------:|----------:|--------:|-------:|:-------:|:--------:|------------|----------|
| 1 | fully | 1 | 1 | 926 | 59986052 | 0 | 0 | no | yes | 4/4 | DIFF(8 lines) |
| 2 | scan_only? | 2 | 0 | 0 | 0 | 0 | 1 | no | no | 100/0 | ON_ERR |
| 3 | fully | 1 | 1 | 1194 | 76486052 | 0 | 0 | yes | yes | 10/10 | DIFF(4 lines) |
| 4 | fully | 1 | 1 | 1164 | 74986052 | 0 | 0 | yes | yes | 5/5 | exact |
| 5 | fully | 1 | 1 | 1199 | 76586082 | 0 | 0 | yes | yes | 5/5 | exact |
| 6 | fully | 1 | 1 | 926 | 59986052 | 0 | 0 | no | no | 1/1 | exact |
| 7 | fully | 1 | 1 | 1200 | 76586102 | 0 | 0 | yes | yes | 4/4 | exact |
| 8 | scan_only? | 1 | 0 | 0 | 0 | 0 | 0 | no | no | 2/0 | ON_ERR |
| 9 | scan_only? | 1 | 0 | 0 | 0 | 0 | 0 | no | no | 175/0 | ON_ERR |
| 10 | fully | 1 | 1 | 1196 | 76486077 | 0 | 0 | yes | yes | 381105/381105 | DIFF(200846 lines) |
| 11 | scan_only? | 2 | 0 | 0 | 0 | 0 | 0 | no | no | 8685/0 | ON_ERR |
| 12 | fully | 1 | 1 | 1165 | 74986052 | 0 | 0 | yes | yes | 2/2 | exact |
| 13 | scan_only | 1 | 1 | 268 | 16500000 | 0 | 1 | yes | yes | 46/46 | exact |
| 14 | scan_only? | 1 | 0 | 0 | 0 | 0 | 0 | no | no | 1/0 | ON_ERR |
| 15 | scan_only? | 2 | 0 | 0 | 0 | 1 | 1 | no | no | 1/0 | ON_ERR |
| 16 | scan_only? | 3 | 0 | 0 | 0 | 1 | 1 | no | no | 27840/0 | ON_ERR |
| 17 | scan_only? | 2 | 0 | 0 | 0 | 0 | 2 | no | no | 1/0 | ON_ERR |
| 18 | scan_only | 1 | 1 | 926 | 59986052 | 3 | 1 | no | yes | 624/624 | exact |
| 19 | fully | 1 | 1 | 963 | 61986052 | 0 | 0 | yes | no | 1/1 | exact |
| 20 | scan_only | 4 | 1 | 5 | 100025 | 1 | 1 | yes | no | 1804/0 | ON_ERR |
| 21 | scan_only? | 2 | 0 | 0 | 0 | 0 | 1 | no | no | 100/0 | ON_ERR |
| 22 | scan_only? | 3 | 0 | 0 | 0 | 0 | 1 | no | no | 7/0 | ON_ERR |
