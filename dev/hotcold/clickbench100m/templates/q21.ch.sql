SELECT count(*) FROM (SELECT * FROM streamed_table('/pgch_1300451_1_1_0', 'url String')) WHERE ((url LIKE '%google%'))
