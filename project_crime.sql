-- Active: 1742332639624@@104.196.195.168@8123@card_data
create database if not EXISTS crime_data;
DROP TABLE IF EXISTS crime_data.chicago_crimes;
CREATE TABLE IF NOT EXISTS crime_data.chicago_crimes (
    ID UInt64,
    `Case Number` String,
    Date String,
    Block String,
    IUCR String,
    `Primary Type` String,
    Description String,
    `Location Description` String,
    Arrest Boolean,
    Domestic Boolean,
    Beat UInt32,
    District Float64,
    Ward Float64,
    `Community Area` Float64,
    `FBI Code` String,
    `X Coordinate` Float64,
    `Y Coordinate` Float64,
    Year UInt16,
    `Updated On` String,
    Latitude Float64,
    Longitude Float64,
    Location String
) ENGINE = MergeTree()
ORDER BY Date;


CREATE TABLE IF NOT EXISTS crime_data.chicago_dist

select * from crime_data.chicago_crimes
LIMIT 100;

Select `Primary Type`, count(*) as crime_count
from crime_data.chicago_crimes
GROUP BY `Primary Type`
ORDER BY crime_count DESC

select Year, count(*) as crime_count
from crime_data.chicago_crimes
GROUP BY Year
ORDER BY Year;


SELECT
    ID,
    `Primary Type`,
    COUNT(*) OVER (PARTITION BY `Primary Type`) AS total_per_type
FROM crime_data.chicago_crimes
LIMIT 500;


SELECT
    ID
    ,`Primary Type`
    ,COUNT(*) OVER (PARTITION BY `Primary Type`) AS total_per_type
    ,ROW_NUMBER() OVER (PARTITION BY `Primary Type` ORDER BY Date) AS row_number_within_type
    ,RANK() OVER (PARTITION BY `Primary Type` ORDER BY Date) AS rank_within_type
    ,DENSE_RANK() OVER (PARTITION BY `Primary Type` ORDER BY Date) AS dense_rank_within_type
FROM crime_data.chicago_crimes
LIMIT 700;



CREATE TABLE IF NOT EXISTS crime_data.nyc_crime (
    ARREST_KEY UInt64,
    ARREST_DATE String,
    PD_CD Float64,
    PD_DESC String,
    KY_CD Float64,
    OFNS_DESC String,
    LAW_CODE String,
    LAW_CAT_CD String,
    ARREST_BORO String,
    ARREST_PRECINCT UInt16,
    JURISDICTION_CODE Float64,
    AGE_GROUP String,
    PERP_SEX String,
    PERP_RACE String,
    X_COORD_CD Float64,
    Y_COORD_CD Float64,
    Latitude Float64,
    Longitude Float64,
    Lon_Lat String
) ENGINE = MergeTree()
ORDER BY ARREST_DATE;

select * from crime_data.nyc_crime

SELECT
    OFNS_DESC,
    COUNT(*) OVER (PARTITION BY OFNS_DESC) AS total_arrests_by_type
FROM crime_data.nyc_crime
LIMIT 100;

SELECT
    OFNS_DESC,
    COUNT(*) AS total_arrests
FROM crime_data.nyc_crime
GROUP BY OFNS_DESC
ORDER BY total_arrests DESC
LIMIT 20;

SELECT
    PERP_RACE
    ,AGE_GROUP
    ,count(*) AS count
    ,rank() OVER (PARTITION BY PERP_RACE ORDER BY COUNT(*)
     DESC) AS rank_within_race
FROM crime_data.nyc_crime
GROUP BY PERP_RACE, AGE_GROUP
ORDER BY count DESC
LIMIT 50;

SELECT
    ARREST_BORO,
    COUNT(*) AS total_arrests
FROM crime_data.nyc_crime;

-- New window function
SELECT
    ARREST_DATE
    ,COUNT(*) AS daily_arrests,
    ,SUM(COUNT(*)) OVER (ORDER BY ARREST_DATE 
    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS cumulative_arrests
FROM crime_data.nyc_crime
GROUP BY ARREST_DATE
ORDER BY ARREST_DATE
LIMIT 100;


CREATE TABLE IF NOT EXISTS  crime_data.nyc_districts  (
    OBJECTID UInt32,
    BoroCD UInt32,
    geom String  -- geometry stored as WKT
) ENGINE = MergeTree
ORDER BY OBJECTID

select * from crime_data.nyc_districts

CREATE TABLE IF NOT EXISTS crime_data.crimes (
    crime_id UInt64,
    ARREST_DATE Nullable(String),
    PD_CD Nullable(UInt16),
    PD_DESC Nullable(String),
    KY_CD Nullable(UInt16),
    LAW_CODE Nullable(String),
    LAW_CAT_CD Nullable(String),
    ARREST_BORO Nullable(String),
    ARREST_PRECINCT Nullable(UInt16),
    JURISDICTION_CODE Nullable(UInt16),
    AGE_GROUP Nullable(String),
    PERP_SEX Nullable(String),
    PERP_RACE Nullable(String),
    x_coord Nullable(Float64),
    y_coord Nullable(Float64),
    Latitude Nullable(Float64),
    Longitude Nullable(Float64),
    description Nullable(String),
    primary_type Nullable(String),
    case_number Nullable(String),
    arrest Nullable(UInt8),
    domestic Nullable(UInt8),
    beat Nullable(UInt16),
    ward Nullable(UInt16),
    fbi_code Nullable(String),
    year Nullable(UInt16),
    district Nullable(UInt16),
    intersects Nullable(UInt8),
    city String
) ENGINE = MergeTree
ORDER BY (crime_id);

SHOW TABLES FROM crime_data;

INSERT INTO crime_data.crimes
SELECT
    ARREST_KEY AS crime_id,
    ARREST_DATE,
    PD_CD,
    PD_DESC,
    KY_CD,
    LAW_CODE,
    LAW_CAT_CD,
    ARREST_BORO,
    ARREST_PRECINCT,
    JURISDICTION_CODE,
    AGE_GROUP,
    PERP_SEX,
    PERP_RACE,
    X_COORD_CD AS x_coord,
    Y_COORD_CD AS y_coord,
    Latitude,
    Longitude,
    OFNS_DESC AS description,
    NULL AS primary_type,
    NULL AS case_number,
    NULL AS arrest,
    NULL AS domestic,
    NULL AS beat,
    NULL AS ward,
    NULL AS fbi_code,
    NULL AS year,
    district,
    intersects,
    'NYC' AS city
FROM crime_data.nyc_crime;
