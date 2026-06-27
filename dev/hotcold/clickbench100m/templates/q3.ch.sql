SELECT sum(advengineid), count(*), avg(resolutionwidth) FROM (SELECT * FROM streamed_table('/pgch_539725_1_1_0', 'resolutionwidth Int16, advengineid Int16'))
