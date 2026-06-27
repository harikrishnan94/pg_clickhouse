SELECT 1, url, count(*) FROM (SELECT * FROM streamed_table('/pgch_1911640_1_1_0', 'url String')) GROUP BY cast(1 as Int32), url ORDER BY count(*) DESC NULLS FIRST LIMIT 10
