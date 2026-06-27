SELECT userid, searchphrase, count(*) FROM (SELECT * FROM streamed_table('/pgch_1186766_1_1_0', 'userid Int64, searchphrase String')) GROUP BY userid, searchphrase LIMIT 10
