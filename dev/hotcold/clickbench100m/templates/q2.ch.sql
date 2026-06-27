SELECT count(*) FROM (SELECT * FROM streamed_table('/pgch_492808_1_1_0', 'advengineid Int16')) WHERE ((advengineid <> 0))
