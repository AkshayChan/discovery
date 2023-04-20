-- -------------------------
-- Riders tracking data 10Hz Sensor data
-- Riders data 1Hz Sensor data
-- All combined into one stream
-- ------------------------
CREATE OR REPLACE STREAM "TELEMETRY_RAW_DATA_ENRICHED" (
 "APPROXIMATE_ARRIVAL_TIME" timestamp,
 "InputMessage" varchar(20),
 "ServerTimeStamp" VARCHAR(32),
 "ApiIngestTime" VARCHAR(32),
 "KinesisAnalyticsIngestTime" timestamp,
 "SeasonID" integer,
 "EventID" integer,
 "RaceID" integer,
 "EventTimeStamp" timestamp,
 "UCIID" bigint,
 "Bib" integer,
 "LeagueCat" VARCHAR(16),
 "RiderRank" integer,
 "State" VARCHAR(4),
 "Distance" double,
 "DistanceProj" double,
 "Speed" double,
 "SpeedMax" double,
 "SpeedAvg" double,
 "DistanceFirst" double,
 "DistanceNext" double,
 "Acc" double,
 "Lat" double,
 "Lng" double,
 "RiderHeartrate" integer,
 "RiderCadency" integer,
 "RiderPower" integer,
 "MaxHrBpm" integer,
 "IsInHeartrateRedZone" integer,
 "IsInHeartrateOrangeZone" integer,
 "IsInHeartrateGreenZone" integer);

CREATE OR REPLACE PUMP "TELEMETRY_RAW_DATA_ENRICHED_PUMP" AS
INSERT INTO "TELEMETRY_RAW_DATA_ENRICHED" (
"APPROXIMATE_ARRIVAL_TIME", "InputMessage",
"ServerTimeStamp", "ApiIngestTime", "KinesisAnalyticsIngestTime", "SeasonID", "EventID", "RaceID", "EventTimeStamp",  "Bib", "LeagueCat", "UCIID", "RiderRank", "State",
 "Distance",
 "DistanceProj",
 "Speed",
 "SpeedMax",
 "SpeedAvg",
 "DistanceFirst",
 "DistanceNext",
 "Acc" ,
 "Lat" ,
 "Lng" ,
 "RiderHeartrate",
 "RiderCadency",
 "RiderPower",
 "MaxHrBpm",
 "IsInHeartrateRedZone",
 "IsInHeartrateOrangeZone",
 "IsInHeartrateGreenZone")
SELECT  STREAM "APPROXIMATE_ARRIVAL_TIME",  "InputMessage", "ServerTimeStamp", "ApiIngestTime", ROWTIME, "SeasonID", "EventID", "RaceID", "EventTimeStamp",  "Bib", "LeagueCat", "SOURCE_SQL_STREAM_001"."UCIID", "RiderRank", "State",
        "Distance",
        "DistanceProj",
        "Speed",
        "SpeedMax",
        "SpeedAvg",
        "DistanceFirst",
        "DistanceNext",
        "Acc" ,
        "Lat" ,
        "Lng",
        "RiderHeartrate",
        "RiderCadency",
        "RiderPower",
        "PSQ"."MaxHrBpm",
        CASE WHEN "RiderHeartrate" > (0.95 * "MaxHrBpm")  THEN 1 ELSE 0 END,
        CASE WHEN "RiderHeartrate" > (0.70 * "MaxHrBpm") AND "RiderHeartrate" <= (0.95 * "MaxHrBpm")  THEN 1 ELSE 0 END,
        CASE WHEN "RiderHeartrate" <= (0.70 * "MaxHrBpm")  THEN 1 ELSE 0 END
FROM "SOURCE_SQL_STREAM_001"  LEFT JOIN "PRE_SEASON_QUESTIONNAIRE" as "PSQ"
                                        ON "SOURCE_SQL_STREAM_001".UCIID = "PSQ".UCIID WHERE "InputMessage" IN ('LiveRidersTracking', 'LiveRidersData');


-- -------------------------
-- StartTime data
-- ------------------------
CREATE OR REPLACE STREAM "STARTTIME_DATA" (
 "APPROXIMATE_ARRIVAL_TIME" timestamp,
 "InputMessage" varchar(20),
 "ApiIngestTime" VARCHAR(32),
 "KinesisAnalyticsIngestTime" timestamp,
 "SeasonID" integer,
 "EventID" integer,
 "RaceID" integer,
 "ServerTimeStamp" VARCHAR(32));

CREATE OR REPLACE PUMP "STARTTIME_DATA_PUMP" AS
INSERT INTO "STARTTIME_DATA" ( "APPROXIMATE_ARRIVAL_TIME", "InputMessage", "ApiIngestTime", "KinesisAnalyticsIngestTime", "SeasonID", "EventID", "RaceID", "ServerTimeStamp")
SELECT  STREAM "APPROXIMATE_ARRIVAL_TIME", "InputMessage","ApiIngestTime", ROWTIME, "SeasonID", "EventID", "RaceID", "ServerTimeStamp"
FROM "SOURCE_SQL_STREAM_001" WHERE "InputMessage" = 'StartTime';

-- -------------------------
-- RaceStartLive data
-- ------------------------
CREATE OR REPLACE STREAM "RACESTARTLIVE_DATA" (
 "APPROXIMATE_ARRIVAL_TIME" timestamp,
 "InputMessage" varchar(20),
 "ApiIngestTime" VARCHAR(32),
 "KinesisAnalyticsIngestTime" timestamp,
 "SeasonID" integer,
 "EventID" integer,
 "RaceID" integer,
 "ServerTimeStamp" VARCHAR(32));

CREATE OR REPLACE PUMP "RACESTARTLIVE_DATA_PUMP" AS
INSERT INTO "RACESTARTLIVE_DATA" ( "APPROXIMATE_ARRIVAL_TIME", "InputMessage", "ApiIngestTime", "KinesisAnalyticsIngestTime", "SeasonID", "EventID", "RaceID", "ServerTimeStamp")
SELECT  STREAM "APPROXIMATE_ARRIVAL_TIME", "InputMessage","ApiIngestTime", ROWTIME, "SeasonID", "EventID", "RaceID", "ServerTimeStamp"
FROM "SOURCE_SQL_STREAM_001" WHERE "InputMessage" = 'RaceStartLive';

-- -------------------------
-- FinishTime data
-- ------------------------
CREATE OR REPLACE STREAM "FINISHTIME_DATA" (
 "APPROXIMATE_ARRIVAL_TIME" timestamp,
 "InputMessage" varchar(20),
 "ApiIngestTime" VARCHAR(32),
 "KinesisAnalyticsIngestTime" timestamp,
 "SeasonID" integer,
 "EventID" integer,
 "RaceID" integer,
 "ServerTimeStamp" VARCHAR(32),
 "RaceTime" double,
 "RaceSpeed" double);


CREATE OR REPLACE PUMP "FINISHTIME_DATA_PUMP" AS
INSERT INTO "FINISHTIME_DATA" ( "APPROXIMATE_ARRIVAL_TIME", "InputMessage", "ApiIngestTime", "KinesisAnalyticsIngestTime", "SeasonID", "EventID", "RaceID", "ServerTimeStamp", "RaceTime", "RaceSpeed")
SELECT  STREAM "APPROXIMATE_ARRIVAL_TIME", "InputMessage","ApiIngestTime", ROWTIME, "SeasonID", "EventID", "RaceID", "ServerTimeStamp", "RaceTime", "RaceSpeed"
FROM "SOURCE_SQL_STREAM_001" WHERE "InputMessage" = 'FinishTime';


-- -------------------------
-- LapCounter data
-- ------------------------
CREATE OR REPLACE STREAM "LAPCOUNTER_DATA" (
 "APPROXIMATE_ARRIVAL_TIME" timestamp,
 "InputMessage" varchar(20),
 "ApiIngestTime" VARCHAR(32),
 "KinesisAnalyticsIngestTime" timestamp,
 "SeasonID" integer,
 "EventID" integer,
 "RaceID" integer,
 "ServerTimeStamp" VARCHAR(32),
 "LapsToGo" integer,
 "DistanceToGo" integer);


CREATE OR REPLACE PUMP "LAPCOUNTER_DATA_PUMP" AS
INSERT INTO "LAPCOUNTER_DATA" ( "APPROXIMATE_ARRIVAL_TIME", "InputMessage", "ApiIngestTime", "KinesisAnalyticsIngestTime", "SeasonID", "EventID", "RaceID", "ServerTimeStamp", "LapsToGo", "DistanceToGo")
SELECT  STREAM "APPROXIMATE_ARRIVAL_TIME", "InputMessage", "ApiIngestTime", ROWTIME, "SeasonID", "EventID", "RaceID", "ServerTimeStamp", "LapsToGo", "DistanceToGo"
FROM "SOURCE_SQL_STREAM_001" WHERE "InputMessage" = 'LapCounter';


-- -------------------------
-- RiderEliminated data
-- ------------------------
CREATE OR REPLACE STREAM "RIDERELIMINATED_DATA" (
 "APPROXIMATE_ARRIVAL_TIME" timestamp,
 "InputMessage" varchar(20),
 "ApiIngestTime" VARCHAR(32),
 "KinesisAnalyticsIngestTime" timestamp,
 "SeasonID" integer,
 "EventID" integer,
 "RaceID" integer,
 "EliminatedRaceName" varchar(50),
 "ServerTimeStamp" VARCHAR(32),
 "Bib" integer,
 "UCIID" bigint,
 "FirstName" varchar(50),
 "LastName" varchar(50),
 "ShortTVName" varchar(50),
 "Team" varchar(30),
 "NOC" varchar(6));


CREATE OR REPLACE PUMP "RIDERELIMINATED_DATA_PUMP" AS
INSERT INTO "RIDERELIMINATED_DATA" ( "APPROXIMATE_ARRIVAL_TIME", "InputMessage", "ApiIngestTime", "KinesisAnalyticsIngestTime", "SeasonID", "EventID", "RaceID", "EliminatedRaceName", "ServerTimeStamp",
                               "Bib", "UCIID", "FirstName", "LastName", "ShortTVName", "Team", "NOC")
SELECT  STREAM "APPROXIMATE_ARRIVAL_TIME", "InputMessage", "ApiIngestTime", ROWTIME, "SeasonID", "EventID", "RaceID", "EliminatedRaceName", "ServerTimeStamp",
        "EliminatedBib", "EliminatedUCIID", "EliminatedFirstName", "EliminatedLastName", "EliminatedShortTVName", "EliminatedTeam", "EliminatedNOC"
FROM "SOURCE_SQL_STREAM_001" WHERE "InputMessage" = 'RiderEliminated';

-- -------------------------
-- OTHER_LIVE_DATA data output for: StartTime, RaceStartLive, FinishTime, LapCounter, RiderEliminated
-- Combined stream
-- ------------------------

CREATE OR REPLACE STREAM "OTHER_LIVE_DATA" (
 -- Shared columns for StartTime, RaceStartLive, FinishTime, LapCounter, RiderEliminated
 -- Common output stream as Kinesis Analytics currently supports only 3 outputs
 "APPROXIMATE_ARRIVAL_TIME" timestamp,
 "InputMessage" varchar(20),
 "ApiIngestTime" VARCHAR(32),
 "KinesisAnalyticsIngestTime" timestamp,
 "SeasonID" integer,
 "EventID" integer,
 "RaceID" integer,
 "ServerTimeStamp" VARCHAR(32),
 -- FinishTime specific
 "RaceTime" double,
 "RaceSpeed" double,
 -- LapCounter Specific
 "LapsToGo" integer,
 "DistanceToGo" integer,
 -- RiderEliminated Specific
 "EliminatedRaceName" varchar(50),
 "Bib" integer,
 "UCIID" bigint,
 "FirstName" varchar(50),
 "LastName" varchar(50),
 "ShortTVName" varchar(50),
 "Team" varchar(30),
 "NOC" varchar(6));

CREATE OR REPLACE PUMP "OTHER_LIVE_DATA_PUMP" AS
INSERT INTO "OTHER_LIVE_DATA" ( "APPROXIMATE_ARRIVAL_TIME", "InputMessage", "ApiIngestTime", "KinesisAnalyticsIngestTime", "SeasonID", "EventID", "RaceID",  "ServerTimeStamp",
                               "RaceTime", "RaceSpeed",
                               "LapsToGo", "DistanceToGo",
                               "EliminatedRaceName", "Bib", "UCIID", "FirstName", "LastName", "ShortTVName", "Team", "NOC")
SELECT  STREAM "APPROXIMATE_ARRIVAL_TIME", "InputMessage", "ApiIngestTime", ROWTIME, "SeasonID", "EventID", "RaceID", "ServerTimeStamp",
        "RaceTime", "RaceSpeed",
        "LapsToGo", "DistanceToGo",
        "EliminatedRaceName", "EliminatedBib", "EliminatedUCIID", "EliminatedFirstName", "EliminatedLastName", "EliminatedShortTVName", "EliminatedTeam", "EliminatedNOC"
FROM "SOURCE_SQL_STREAM_001" WHERE "InputMessage" IN ('StartTime', 'RaceStartLive', 'FinishTime', 'LapCounter', 'RiderEliminated');

----------
-- EXPERIMENT with speed
---------

CREATE OR REPLACE STREAM "TEMP_SPEED" (
 "EventTimeStamp" timestamp,
 "InputMessage" varchar(20),
 "ApiIngestTime" VARCHAR(32),
 "KinesisAnalyticsIngestTime" timestamp,
 "SeasonID" integer,
 "EventID" integer,
 "RaceID" integer,
 "UCIID" bigint,
 "AvgRiderSpeed" double,
 "RiderRank" integer
 );

CREATE OR REPLACE PUMP "TEMP_SPEED_PUMP" AS
INSERT INTO "TEMP_SPEED"(
 "EventTimeStamp",
 "InputMessage",
 "ApiIngestTime",
 "KinesisAnalyticsIngestTime",
 "SeasonID",
 "EventID",
 "RaceID",
 "UCIID",
 "AvgRiderSpeed",
 "RiderRank"
)
SELECT STREAM STEP("SOURCE_SQL_STREAM_001"."EventTimeStamp" BY INTERVAL '1' SECOND) as "EventTimeStamp",
       MAX('AggTempSpeed'),
       FIRST_VALUE("ApiIngestTime"),
       FIRST_VALUE(SOURCE_SQL_STREAM_001.ROWTIME),
       "SeasonID",
       "EventID",
       "RaceID",
       "UCIID",
       AVG("Speed") AS "AvgRiderSpeed",
       LAST_VALUE("RiderRank") AS "RiderRank"
FROM     "SOURCE_SQL_STREAM_001"
WHERE "InputMessage" = 'LiveRidersTracking'
GROUP BY "SeasonID", "EventID", "RaceID", "UCIID",
         STEP("SOURCE_SQL_STREAM_001".ROWTIME BY INTERVAL '1' SECOND),
         STEP("SOURCE_SQL_STREAM_001"."EventTimeStamp" BY INTERVAL '1' SECOND);


-- -------------------------
-- AGGREGATES
-- it has data from LiveRidersData and downsampled speed from TEMP_SPEED
-- ------------------------
CREATE OR REPLACE STREAM "AGGREGATES" (
 "InputMessage" varchar(20),
 "ApiIngestTime" VARCHAR(32),
 "KinesisAnalyticsIngestTime" timestamp,
 "EventTimeStamp" timestamp,
 "SeasonID" integer,
 "EventID" integer,
 "RaceID" integer,
 "UCIID" bigint,
 "Bib" integer,
 "LeagueCat" varchar(16),
 "RiderHeartrate" integer,
 "RiderHeartrateExceeded" integer,
 "AvgRaceRiderHeartrate" integer,
 "MaxRaceRiderHeartrate" integer,
 "MaxRaceHeartrate" integer,
 "RiderCadency" integer,
 "AvgRaceRiderCadency" integer,
 "MaxRaceRiderCadency" integer,
 "MaxRaceCadency" integer,
 "RiderPower" integer,
 "RiderPowerExceeded" integer,
 "AvgRaceRiderPower" integer,
 "MaxRaceRiderPower" integer,
 "MaxRacePower" integer,
 "Power5s" integer,
 "Power5sExceeded" integer,
 "Power15s" integer,
 "Power15sExceeded" integer,
 "Power30s" integer,
 "Power30sExceeded" integer,
 "Power60s" integer,
 "Power60sExceeded" integer,
 "Power120s" integer,
 "Power120sExceeded" integer,
 "Power180s" integer,
 "Power180sExceeded" integer,
 "Power300s" integer,
 "Power300sExceeded" integer,
 "Power600s" integer,
 "Power600sExceeded" integer,
 "RiderSpeed" integer,
 "AvgRaceRiderSpeed" integer,
 "MaxRaceRiderSpeed" integer,
 "MaxRaceSpeed" integer,
 "IsInHeartrateRedZone" integer,
 "TimeSpentInRedZone" integer,
 "IsInHeartrateOrangeZone" integer,
 "TimeSpentInOrangeZone" integer,
 "IsInHeartrateGreenZone" integer,
 "TimeSpentInGreenZone" integer,
 "RiderRank" integer
);


CREATE OR REPLACE PUMP "AGGREGATES_PUMP" AS
INSERT INTO "AGGREGATES"(
 "InputMessage",
 "ApiIngestTime",
 "KinesisAnalyticsIngestTime",
 "EventTimeStamp",
 "SeasonID",
 "EventID",
 "RaceID",
 "UCIID",
 "Bib",
 "LeagueCat",
 "RiderHeartrate",
 "AvgRaceRiderHeartrate",
 "MaxRaceRiderHeartrate",
 "MaxRaceHeartrate",
 "RiderCadency",
 "AvgRaceRiderCadency",
 "MaxRaceRiderCadency",
 "MaxRaceCadency",
 "RiderPower",
 "AvgRaceRiderPower",
 "MaxRaceRiderPower",
 "MaxRacePower",
 "IsInHeartrateRedZone",
 "TimeSpentInRedZone",
 "IsInHeartrateOrangeZone",
 "TimeSpentInOrangeZone",
 "IsInHeartrateGreenZone",
 "TimeSpentInGreenZone"
)
SELECT 'AggRidersData',
       "ApiIngestTime",
       "KinesisAnalyticsIngestTime",
       "EventTimeStamp",
       "SeasonID",
       "EventID",
       "RaceID",
       "UCIID",
       "Bib",
       "LeagueCat",
       "RiderHeartrate",
       AVG("RiderHeartrate") OVER (
            PARTITION BY "RaceID", "UCIID"
            RANGE INTERVAL '30' MINUTE PRECEDING) AS AvgRaceRiderHeartrate,
        MAX("RiderHeartrate") OVER (
            PARTITION BY "RaceID", "UCIID"
            RANGE INTERVAL '30' MINUTE PRECEDING) AS MaxRaceRiderHeartrate,
       MAX("RiderHeartrate") OVER (
            PARTITION BY "RaceID"
            RANGE INTERVAL '30' MINUTE PRECEDING) AS MaxRaceHeartrate,
       "RiderCadency",
       AVG("RiderCadency") OVER (
            PARTITION BY "RaceID", "UCIID"
            RANGE INTERVAL '30' MINUTE PRECEDING) AS AvgRaceRiderCadency,
       MAX("RiderCadency") OVER (
            PARTITION BY "RaceID", "UCIID"
            RANGE INTERVAL '30' MINUTE PRECEDING) AS MaxRaceRiderCadency,
       MAX("RiderCadency") OVER (
            PARTITION BY "RaceID"
            RANGE INTERVAL '30' MINUTE PRECEDING) AS MaxRaceCadency,
       "RiderPower",
       AVG("RiderPower") OVER (
            PARTITION BY "RaceID", "UCIID"
            RANGE INTERVAL '30' MINUTE PRECEDING) AS AvgRaceRiderPower,
       MAX("RiderPower") OVER (
            PARTITION BY "RaceID", "UCIID"
            RANGE INTERVAL '30' MINUTE PRECEDING) AS MaxRaceRiderPower,
       MAX("RiderPower") OVER (
            PARTITION BY "RaceID"
            RANGE INTERVAL '30' MINUTE PRECEDING) AS MaxRacePower,
       "IsInHeartrateRedZone",
       SUM("IsInHeartrateRedZone") OVER (
            PARTITION BY "RaceID", "UCIID"
            RANGE INTERVAL '30' MINUTE PRECEDING) AS TimeSpentInRedZone,
       "IsInHeartrateOrangeZone",
       SUM("IsInHeartrateOrangeZone") OVER (
            PARTITION BY "RaceID", "UCIID"
            RANGE INTERVAL '30' MINUTE PRECEDING) AS TimeSpentInOrangeZone,
       "IsInHeartrateGreenZone",
       SUM("IsInHeartrateGreenZone") OVER (
            PARTITION BY "RaceID", "UCIID"
            RANGE INTERVAL '30' MINUTE PRECEDING) AS TimeSpentInGreenZone
FROM "TELEMETRY_RAW_DATA_ENRICHED" WHERE "InputMessage" = 'LiveRidersData';

CREATE OR REPLACE PUMP "AGGREGATES_PUMP2" AS
INSERT INTO "AGGREGATES"(
 "InputMessage",
 "ApiIngestTime",
 "KinesisAnalyticsIngestTime",
 "EventTimeStamp",
 "SeasonID",
 "EventID",
 "RaceID",
 "UCIID",
 "RiderSpeed",
 "AvgRaceRiderSpeed",
 "MaxRaceRiderSpeed",
 "MaxRaceSpeed",
 "RiderRank"
)
SELECT 'AggRidersTracking',
       "ApiIngestTime",
       "KinesisAnalyticsIngestTime",
       "EventTimeStamp",
       "SeasonID",
       "EventID",
       "RaceID",
       "UCIID",
       "AvgRiderSpeed",
       AVG("AvgRiderSpeed") OVER (
            PARTITION BY "RaceID", "UCIID"
            RANGE INTERVAL '30' MINUTE PRECEDING) AS AvgRaceRiderSpeed,
       MAX("AvgRiderSpeed") OVER (
            PARTITION BY "RaceID", "UCIID"
            RANGE INTERVAL '30' MINUTE PRECEDING) AS MaxRaceRiderSpeed,
       MAX("AvgRiderSpeed") OVER (
            PARTITION BY "RaceID"
            RANGE INTERVAL '30' MINUTE PRECEDING) AS MaxRaceSpeed,
       "RiderRank"
FROM "TEMP_SPEED";

CREATE OR REPLACE PUMP "PERSONAL_BEST_AGGREGATES_PUMP" AS
INSERT INTO "AGGREGATES"(
 "InputMessage",
 "ApiIngestTime",
 "KinesisAnalyticsIngestTime",
 "EventTimeStamp",
 "SeasonID",
 "EventID",
 "RaceID",
 "UCIID",
 "Bib",
 "LeagueCat",
 "RiderHeartrate",
 "RiderHeartrateExceeded",
 "RiderPower",
 "RiderPowerExceeded",
 "Power5s",
 "Power5sExceeded",
 "Power15s",
 "Power15sExceeded",
 "Power30s",
 "Power30sExceeded",
 "Power60s",
 "Power60sExceeded",
 "Power120s",
 "Power120sExceeded",
 "Power180s",
 "Power180sExceeded",
 "Power300s",
 "Power300sExceeded",
 "Power600s",
 "Power600sExceeded"
)
SELECT 'AggPersonalBest',
       "PBA"."ApiIngestTime",
       "PBA"."KinesisAnalyticsIngestTime",
       "PBA"."EventTimeStamp",
       "PBA"."SeasonID",
       "PBA"."EventID",
       "PBA"."RaceID",
       "PBA"."UCIID",
       "PBA"."Bib",
       "PBA"."LeagueCat",
       "PBA"."RiderHeartrate",
       CASE WHEN "PBA"."RiderHeartrate" > "PSQ"."MaxHrBpm" THEN 1 ELSE 0 END,
       "PBA"."RiderPower",
       CASE WHEN "PBA"."RiderPower" > "PSQ"."PowerPeakW" THEN 1 ELSE 0 END,
       "PBA"."Power5s",
       CASE WHEN "PBA"."Power5s" > "PSQ"."Power5sW" THEN 1 ELSE 0 END,
       "PBA"."Power15s",
       CASE WHEN "PBA"."Power15s" > "PSQ"."Power15sW" THEN 1 ELSE 0 END,
       "PBA"."Power30s",
       CASE WHEN "PBA"."Power30s" > "PSQ"."Power30sW" THEN 1 ELSE 0 END,
       "PBA"."Power60s",
       CASE WHEN "PBA"."Power60s" > "PSQ"."Power60sW" THEN 1 ELSE 0 END,
       "PBA"."Power120s",
       CASE WHEN "PBA"."Power120s" > "PSQ"."Power120sW" THEN 1 ELSE 0 END,
       "PBA"."Power180s",
       CASE WHEN "PBA"."Power180s" > "PSQ"."Power180sW" THEN 1 ELSE 0 END,
       "PBA"."Power300s",
       CASE WHEN "PBA"."Power300s" > "PSQ"."Power300sW" THEN 1 ELSE 0 END,
       "PBA"."Power600s",
       CASE WHEN "PBA"."Power600s" > "PSQ"."Power600sW" THEN 1 ELSE 0 END
FROM (
  SELECT "InputMessage",
         "ApiIngestTime",
         "KinesisAnalyticsIngestTime",
         "EventTimeStamp",
         "SeasonID",
         "EventID",
         "RaceID",
         "UCIID",
         "Bib",
         "LeagueCat",
         "RiderHeartrate",
         "RiderPower",
         AVG("RiderPower") OVER (
             PARTITION BY "RaceID", "UCIID"
             ROWS 4 PRECEDING) AS "Power5s",
         AVG("RiderPower") OVER (
             PARTITION BY "RaceID", "UCIID"
             ROWS 14 PRECEDING) AS "Power15s",
         AVG("RiderPower") OVER (
             PARTITION BY "RaceID", "UCIID"
             ROWS 29 PRECEDING) AS "Power30s",
         AVG("RiderPower") OVER (
             PARTITION BY "RaceID", "UCIID"
             ROWS 59 PRECEDING) AS "Power60s",
         AVG("RiderPower") OVER (
             PARTITION BY "RaceID", "UCIID"
             ROWS 119 PRECEDING) AS "Power120s",
         AVG("RiderPower") OVER (
             PARTITION BY "RaceID", "UCIID"
             ROWS 179 PRECEDING) AS "Power180s",
         AVG("RiderPower") OVER (
             PARTITION BY "RaceID", "UCIID"
             ROWS 299 PRECEDING) AS "Power300s",
         AVG("RiderPower") OVER (
             PARTITION BY "RaceID", "UCIID"
             ROWS 599 PRECEDING) AS "Power600s"
  FROM "TELEMETRY_RAW_DATA_ENRICHED") AS PBA LEFT JOIN "PRE_SEASON_QUESTIONNAIRE" as "PSQ"
                                             ON "PBA".UCIID = "PSQ".UCIID WHERE "PBA"."InputMessage" = 'LiveRidersData';
