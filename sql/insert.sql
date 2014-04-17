--
-- test Insert with and without indices
--

-- create relations
CREATE TABLE stars(starid integer, real_name char(20), 
                   plays char(12), soapid integer);


-- insert with attributes in order:
INSERT INTO stars(starid, real_name, plays, soapid) 
	VALUES (100, 'Posey, Parker', 'Tess', 6);

-- insert with attributes out of order:
INSERT INTO stars (real_name, soapid, starid, plays) 
	VALUES ('Bonarrigo, Laura', 3, 101, 'Cassie');

--
-- test Insert with indices
--
CREATE INDEX stars (starid);
CREATE INDEX stars (soapid);
CREATE INDEX stars (plays);

INSERT INTO stars (real_name, soapid, starid, plays) 
	VALUES ('Bonarrigo, Laura', 11, 183, 'Cassie');

INSERT INTO stars (soapid, starid, plays,real_name) 
	VALUES (10, 69,'Spiderman' ,'Christen');

DROP TABLE stars;
