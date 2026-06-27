SELECT min(eventdate), max(eventdate) FROM (SELECT * FROM streamed_table('/pgch_713128_1_3_0', 'eventdate Date'))
