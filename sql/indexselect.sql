-- setup the database
CREATE TABLE stars(starid integer, real_name char(20), 
                   plays char(12), soapid integer);
CREATE INDEX stars (starid);
INSERT INTO stars(starid, real_name, plays, soapid) 
	VALUES (100, 'Posey, Parker', 'Tess', 6);
INSERT INTO stars (real_name, soapid, starid, plays) 
	VALUES ('Bonarrigo, Laura', 3, 100, 'Cassie');
INSERT INTO stars (real_name, soapid, starid, plays) 
	VALUES ('John, Snow', 11, 100, 'mamamia');
INSERT INTO stars (soapid, starid, plays,real_name) 
	VALUES (10, 100,'Spiderman' ,'Christen');


--test on predicate: EQ, should select indexselect
SELECT * FROM stars WHERE stars.starid = 100;

--projection order different from schema
SELECT stars.plays, stars.real_name FROM stars WHERE stars.starid = 100;

--projection order same as schema
SELECT stars.starid, stars.real_name FROM stars WHERE stars.starid = 100;

DROP TABLE stars;
