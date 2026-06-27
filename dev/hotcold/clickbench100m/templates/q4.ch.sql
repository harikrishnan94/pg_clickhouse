SELECT avg(toFloat64(userid)) FROM (SELECT * FROM streamed_table('/pgch_586978_1_1_0', 'userid Int64'))
