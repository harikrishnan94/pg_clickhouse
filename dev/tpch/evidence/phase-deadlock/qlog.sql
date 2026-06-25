SELECT event_time, type, read_rows, ProfileEvents['ShmAdoptedBlocks'] AS shm_blocks,
       result_rows, substring(exception,1,160) AS exc,
       substring(replaceRegexpAll(query,'\\s+',' '),1,110) AS q
FROM system.query_log
WHERE log_comment={tag:String}
  AND positionCaseInsensitive(query,'query_log')=0
ORDER BY event_time_microseconds DESC LIMIT 6 FORMAT TSV
