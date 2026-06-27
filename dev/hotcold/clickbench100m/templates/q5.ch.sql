SELECT count(DISTINCT userid) FROM (SELECT * FROM streamed_table('/pgch_629801_1_1_0', 'userid Int64'))
