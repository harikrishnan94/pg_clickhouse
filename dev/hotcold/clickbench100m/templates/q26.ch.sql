SELECT searchphrase FROM (SELECT * FROM streamed_table('/pgch_1580174_1_1_0', 'searchphrase String')) WHERE ((searchphrase <> '')) ORDER BY searchphrase ASC NULLS LAST LIMIT 10
