SELECT advengineid, count(*) FROM (SELECT * FROM streamed_table('/pgch_756235_1_1_0', 'advengineid Int16')) WHERE ((advengineid <> 0)) GROUP BY advengineid ORDER BY count(*) DESC NULLS FIRST
