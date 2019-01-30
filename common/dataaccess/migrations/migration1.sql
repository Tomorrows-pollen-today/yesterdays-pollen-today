CREATE TABLE IF NOT EXISTS PollenArchive_temp (
    Date TIMESTAMP,
    PollenType INT,
    Location INT,
    PollenCount INT, 
    PredictedPollenCount FLOAT,
    PRIMARY KEY (Date, PollenType, Location)
);
CREATE TABLE IF NOT EXISTS Locations (
    Location INT PRIMARY KEY,
    Country VARCHAR,
    City VARCHAR
);
CREATE INDEX IF NOT EXISTS LocationLookup ON Locations (
    Country ASC,
    City ASC
);

INSERT INTO Locations (Location, Country, City) VALUES (0, 'Denmark', 'Copenhagen');

INSERT INTO PollenArchive_temp (Date, PollenType, Location, PollenCount, PredictedPollenCount) (
    SELECT Date, 0, 0, PollenCount, PredictedPollenCount 
    FROM PollenArchive
);

DROP TABLE PollenArchive;

CREATE TABLE PollenArchive (
    Date TIMESTAMP,
    PollenType INT,
    Location INT,
    PollenCount INT, 
    PredictedPollenCount FLOAT,
    PRIMARY KEY (Date, PollenType, Location)
);

INSERT INTO PollenArchive (Date, PollenType, Location, PollenCount, PredictedPollenCount) (
    SELECT Date, 0, 0, PollenCount, PredictedPollenCount 
    FROM PollenArchive_temp
);

DROP TABLE PollenArchive_temp;
