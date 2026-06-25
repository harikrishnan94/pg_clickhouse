| Q | verdict | shmscan | strmd_q | ShmBlocks | read_rows | pg_join | pg_agg | ch_join | ch_grpby | rows off/on | fidelity |
|---|---------|--------:|--------:|----------:|----------:|--------:|-------:|:-------:|:--------:|------------|----------|
| 1 | fully | 1 | 1 | 926 | 59986052 | 0 | 0 | no | yes | 4/4 | DIFF(8 lines) |
| 3 | scan_only? | 1 | 0 | 0 | 0 | 0 | 0 | no | no | 10/10 | DIFF(4 lines) |
| 4 | fully | 1 | 1 | 1164 | 74986052 | 0 | 0 | yes | yes | 5/5 | exact |
| 5 | scan_only? | 1 | 0 | 0 | 0 | 0 | 0 | no | no | 5/5 | exact |
| 6 | fully | 1 | 1 | 926 | 59986052 | 0 | 0 | no | no | 1/1 | exact |
| 7 | fully | 1 | 1 | 1198 | 76586102 | 0 | 0 | yes | yes | 4/4 | exact |
| 10 | fully | 1 | 1 | 1196 | 76486077 | 0 | 0 | yes | yes | 381105/381105 | DIFF(200846 lines) |
| 12 | fully | 1 | 1 | 1165 | 74986052 | 0 | 0 | yes | yes | 2/2 | exact |
| 19 | fully | 1 | 1 | 962 | 61986052 | 0 | 0 | yes | no | 1/1 | exact |
