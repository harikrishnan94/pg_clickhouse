SELECT url, count(*) FROM (SELECT * FROM streamed_table('/pgch_1881902_1_1_0', 'url String')) GROUP BY url ORDER BY count(*) DESC NULLS FIRST LIMIT 10
