SELECT count(DISTINCT searchphrase) FROM (SELECT * FROM streamed_table('/pgch_671275_1_1_0', 'searchphrase String'))
