| Q | verdict | shmscan | strmd_q | ShmBlocks | read_rows | pg_join | pg_agg | ch_join | ch_grpby | rows off/on | fidelity |
|---|---------|--------:|--------:|----------:|----------:|--------:|-------:|:-------:|:--------:|------------|----------|
| 8 | fully | 1 | 1 | 1238 | 78586107 | 0 | 0 | yes | yes | 2/2 | DIFF(4 lines) |
| 9 | scan_only? | 1 | 0 | 0 | 0 | 0 | 0 | no | no | 175/175 | DIFF(26 lines) |
| 11 | scan_only? | 2 | 0 | 0 | 0 | 0 | 0 | no | no | 8685/0 | ON_ERR |
| 14 | fully | 1 | 1 | 961 | 61986052 | 0 | 0 | yes | no | 1/1 | DIFF(2 lines) |
