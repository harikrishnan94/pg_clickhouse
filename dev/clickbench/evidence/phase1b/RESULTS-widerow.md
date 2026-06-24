Q24 SELECT * (105 cols) offload after raising SHM col limit 64->128:
  fidelity: exact|10|10|0|0 (tie-robust ORDER BY EventTime,WatchID)
  oracle: QueryFinish read_rows=10000000 ShmAdoptedBlocks=499
  PG plan: Limit -> Sort -> Custom Scan (ClickHouseShmScan) [filter pushed; top-N in PG]
  consumer unit_tests_dbms SharedMemory/Adoption/Wire: 49 PASSED
  sanity.sh: PASS=17 FAIL=0; zero /dev/shm/pgch_* leaks; zero stray workers
