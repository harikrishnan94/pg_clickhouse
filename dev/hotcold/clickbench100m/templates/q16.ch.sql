SELECT userid, count(*) FROM (SELECT * FROM streamed_table('/pgch_1109610_1_1_0', 'userid Int64')) GROUP BY userid ORDER BY count(*) DESC NULLS FIRST LIMIT 10
