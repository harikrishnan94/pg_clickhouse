SELECT type, read_rows, ProfileEvents['ShmAdoptedBlocks'] AS shm_blocks, result_rows, substring(replaceRegexpAll(query,'\\s+',' '),1,120) AS q
FROM system.query_log
WHERE log_comment={tag:String} AND type='QueryFinish'
  AND positionCaseInsensitive(query,'streamed_table')>0
  AND positionCaseInsensitive(query,'query_log')=0
ORDER BY event_time_microseconds DESC LIMIT 3 FORMAT TSV
