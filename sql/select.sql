-- setup the database
CREATE TABLE stars(starid integer, real_name char(20), 
                   plays char(12), soapid integer);
CREATE INDEX stars (starid);
INSERT INTO stars(starid, real_name, plays, soapid) 
	VALUES (100, 'Posey, Parker', 'Tess', 6);
INSERT INTO stars (real_name, soapid, starid, plays) 
	VALUES ('Bonarrigo, Laura', 3, 101, 'Cassie');
INSERT INTO stars (real_name, soapid, starid, plays) 
	VALUES ('Bonarrigo, Laura', 11, 183, 'Cassie');
INSERT INTO stars (soapid, starid, plays,real_name) 
	VALUES (10, 69,'Spiderman' ,'Christen');

--test on predicate: GT, should select scanselect
SELECT * FROM stars WHERE stars.starid > 100;

--test on predicate: GTE, should select scanselect
SELECT * FROM stars WHERE stars.starid >= 100;

--test on predicate: EQ, should select indexselect
SELECT * FROM stars WHERE stars.starid = 100;

--test on predicate: LT, should select scanselect
SELECT * FROM stars WHERE stars.starid < 100;

--test on predicate: LTE, should select scanselect
SELECT * FROM stars WHERE stars.starid <= 100;

--test on predicate: NE, should select scanselect
SELECT * FROM stars WHERE stars.starid <> 100;

--test on predicate: NOTSET, should select scanselect
SELECT * FROM stars;

--test on predicate: EQ, should select scanselect because not indexed
SELECT * FROM stars WHERE stars.soapid = 10;

DROP TABLE stars;
