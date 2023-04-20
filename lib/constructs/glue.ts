import * as cdk from '@aws-cdk/core';
import * as s3 from '@aws-cdk/aws-s3';
import * as ddb from '@aws-cdk/aws-dynamodb';
import * as glue from '@aws-cdk/aws-glue';
import * as iam from '@aws-cdk/aws-iam';
import { envSpecific } from './helpers';

export interface GlueProps {
  readonly s3raw: s3.IBucket;
  readonly s3staging: s3.IBucket;
  readonly s3poststaging: s3.IBucket;
  readonly s3Temp: s3.IBucket;
  readonly ddbTableStatic: ddb.ITable;
}

export class Glue extends cdk.Construct {
  constructor(scope: cdk.Construct, id: string, props: GlueProps) {
    super(scope, id);

    let tableName: string = '';
    let jobName: string = '';

    /*
    ####################
    # Database Definition
    ####################
    */
    // Database for raw data
    const glueDBraw = new glue.Database(this, 'glueDBraw', {
      databaseName: envSpecific('eurosport_cycling_raw', true),
    });

    // Database for Staging
    const glueDBstage = new glue.Database(this, 'glueDBstaging', {
      databaseName: envSpecific('eurosport_cycling_staging', true),
    });

    // Database for Poststaging
    const glueDBpoststage = new glue.Database(this, 'glueDBpoststaging', {
      databaseName: envSpecific('eurosport_cycling_poststaging', true),
    });

    /*
    ####################
    # Table Defintion
    ####################
    */
    tableName = 'PreSeasonQuestionnaire';
    new glue.Table(this, tableName, {
      database: glueDBraw,
      tableName: tableName.toLowerCase(),
      bucket: props.s3raw,
      s3Prefix: `batch_data/${tableName}`,
      columns: [
        {
          name: 'FirstName',
          type: glue.Schema.STRING,
        }, {
          name: 'LastName',
          type: glue.Schema.STRING,
        }, {
          name: 'BirthDate',
          type: glue.Schema.STRING,
        }, {
          name: 'UCIID',
          type: glue.Schema.BIG_INT,
        }, {
          name: 'Gender',
          type: glue.Schema.STRING,
        }, {
          name: 'TrainingLocation',
          type: glue.Schema.STRING,
        }, {
          name: 'LeagueCat',
          type: glue.Schema.STRING,
        }, {
          name: 'Nationality',
          type: glue.Schema.STRING,
        }, {
          name: 'SeasonTitle',
          type: glue.Schema.STRING,
        }, {
          name: 'HeightCm',
          type: glue.Schema.STRING,
        }, {
          name: 'WeightKg',
          type: glue.Schema.STRING,
        }, {
          name: 'RestHrBpm',
          type: glue.Schema.STRING,
        }, {
          name: 'MaxHrBpm',
          type: glue.Schema.STRING,
        }, {
          name: 'Flying200',
          type: glue.Schema.STRING,
        }, {
          name: 'GearingForFlying200',
          type: glue.Schema.STRING,
        }, {
          name: 'PowerPeakW',
          type: glue.Schema.STRING,
        }, {
          name: 'Power5sW',
          type: glue.Schema.STRING,
        }, {
          name: 'Power15sW',
          type: glue.Schema.STRING,
        }, {
          name: 'Power30sW',
          type: glue.Schema.STRING,
        }, {
          name: 'Power60sW',
          type: glue.Schema.STRING,
        }, {
          name: 'Power120sW',
          type: glue.Schema.STRING,
        }, {
          name: 'Power180sW',
          type: glue.Schema.STRING,
        }, {
          name: 'Power300sW',
          type: glue.Schema.STRING,
        }, {
          name: 'Power600sW',
          type: glue.Schema.STRING,
        }, {
          name: 'Power1200sW',
          type: glue.Schema.STRING,
        }, {
          name: 'Power1800sW',
          type: glue.Schema.STRING,
        }],
      partitionKeys: [{
        name: 'date_part',
        type: glue.Schema.STRING,
      }],
      dataFormat: glue.DataFormat.JSON,
    });

    // Table with the raw races list
    tableName = 'RacesList';
    new glue.Table(this, tableName, {
      database: glueDBraw,
      tableName: tableName.toLowerCase(),
      bucket: props.s3raw,
      s3Prefix: `batch_data/${tableName}`,
      columns: [
        {
          name: 'Message',
          type: glue.Schema.STRING,
        }, {
          name: 'SeasonID',
          type: glue.Schema.INTEGER,
        }, {
          name: 'EventID',
          type: glue.Schema.INTEGER,
        }, {
          name: 'TimeStamp',
          type: glue.Schema.STRING,
        }, {
          name: 'Date',
          type: glue.Schema.STRING,
        }, {
          name: 'EventName',
          type: glue.Schema.STRING,
        }, {
          name: 'Races',
          type: glue.Schema.array(glue.Schema.struct([
            {
              name: 'RaceID',
              type: glue.Schema.INTEGER,
            }, {
              name: 'RaceType',
              type: glue.Schema.STRING,
            }, {
              name: 'Gender',
              type: glue.Schema.STRING,
            }, {
              name: 'League',
              type: glue.Schema.STRING,
            }, {
              name: 'Heat',
              type: glue.Schema.INTEGER,
            }, {
              name: 'TotalHeats',
              type: glue.Schema.INTEGER,
            }, {
              name: 'Round',
              type: glue.Schema.INTEGER,
            }, {
              name: 'TotalRounds',
              type: glue.Schema.INTEGER,
            }, {
              name: 'Laps',
              type: glue.Schema.INTEGER,
            }, {
              name: 'Distance',
              type: glue.Schema.INTEGER,
            }, {
              name: 'RaceName',
              type: glue.Schema.STRING,
            }, {
              name: 'StartTime',
              type: glue.Schema.STRING,
            }])),
          comment: 'Nested object with structure',
        }],
      partitionKeys: [{
        name: 'date_part',
        type: glue.Schema.STRING,
      }],
      dataFormat: glue.DataFormat.JSON,
    });

    // Table with the raw race start list
    tableName = 'RaceStartList';
    new glue.Table(this, tableName, {
      database: glueDBraw,
      tableName: tableName.toLowerCase(),
      bucket: props.s3raw,
      s3Prefix: `batch_data/${tableName}`,
      columns: [
        {
          name: 'Message',
          type: glue.Schema.STRING,
        }, {
          name: 'SeasonID',
          type: glue.Schema.INTEGER,
        }, {
          name: 'EventID',
          type: glue.Schema.INTEGER,
        }, {
          name: 'RaceID',
          type: glue.Schema.INTEGER,
        }, {
          name: 'Gender',
          type: glue.Schema.STRING,
        }, {
          name: 'League',
          type: glue.Schema.STRING,
        }, {
          name: 'RaceType',
          type: glue.Schema.STRING,
        }, {
          name: 'Heat',
          type: glue.Schema.INTEGER,
        }, {
          name: 'TotalHeats',
          type: glue.Schema.INTEGER,
        }, {
          name: 'Round',
          type: glue.Schema.INTEGER,
        }, {
          name: 'TotalRounds',
          type: glue.Schema.INTEGER,
        }, {
          name: 'RaceName',
          type: glue.Schema.STRING,
        }, {
          name: 'Laps',
          type: glue.Schema.INTEGER,
        }, {
          name: 'Distance',
          type: glue.Schema.INTEGER,
        }, {
          name: 'TimeStamp',
          type: glue.Schema.STRING,
        }, {
          name: 'Startlist',
          type: glue.Schema.array(glue.Schema.struct([
            {
              name: 'Bib',
              type: glue.Schema.STRING,
            }, {
              name: 'UCIID',
              type: glue.Schema.BIG_INT,
            }, {
              name: 'FirstName',
              type: glue.Schema.STRING,
            }, {
              name: 'LastName',
              type: glue.Schema.STRING,
            }, {
              name: 'ShortTVName',
              type: glue.Schema.STRING,
            }, {
              name: 'Team',
              type: glue.Schema.STRING,
            }, {
              name: 'NOC',
              type: glue.Schema.STRING,
            }, {
              name: 'Status',
              type: glue.Schema.STRING,
            }, {
              name: 'StartPosition',
              type: glue.Schema.STRING,
            }, {
              name: 'StartLane',
              type: glue.Schema.INTEGER,
            }])),
          comment: 'Nested object with structure',
        }],
      partitionKeys: [{
        name: 'date_part',
        type: glue.Schema.STRING,
      }, {
        name: 'race_part',
        type: glue.Schema.STRING,
      }],
      dataFormat: glue.DataFormat.JSON,
    });

    // Table with the raw race result list
    tableName = 'RaceResults';
    new glue.Table(this, tableName, {
      database: glueDBraw,
      tableName: tableName.toLowerCase(),
      bucket: props.s3raw,
      s3Prefix: `batch_data/${tableName}`,
      columns: [
        {
          name: 'Message',
          type: glue.Schema.STRING,
        }, {
          name: 'SeasonID',
          type: glue.Schema.INTEGER,
        }, {
          name: 'EventID',
          type: glue.Schema.INTEGER,
        }, {
          name: 'RaceID',
          type: glue.Schema.INTEGER,
        }, {
          name: 'RaceType',
          type: glue.Schema.STRING,
        }, {
          name: 'Gender',
          type: glue.Schema.STRING,
        }, {
          name: 'League',
          type: glue.Schema.STRING,
        }, {
          name: 'Heat',
          type: glue.Schema.INTEGER,
        }, {
          name: 'TotalHeats',
          type: glue.Schema.INTEGER,
        }, {
          name: 'Round',
          type: glue.Schema.INTEGER,
        }, {
          name: 'TotalRounds',
          type: glue.Schema.INTEGER,
        }, {
          name: 'State',
          type: glue.Schema.STRING,
        }, {
          name: 'RaceName',
          type: glue.Schema.STRING,
        }, {
          name: 'Laps',
          type: glue.Schema.INTEGER,
        }, {
          name: 'Distance',
          type: glue.Schema.INTEGER,
        }, {
          name: 'RaceTime',
          type: glue.Schema.DOUBLE,
        }, {
          name: 'RaceSpeed',
          type: glue.Schema.DOUBLE,
        }, {
          name: 'TimeStamp',
          type: glue.Schema.STRING,
        }, {
          name: 'Results',
          type: glue.Schema.array(glue.Schema.struct([
            {
              name: 'Rank',
              type: glue.Schema.INTEGER,
            }, {
              name: 'Bib',
              type: glue.Schema.STRING,
            }, {
              name: 'UCIID',
              type: glue.Schema.BIG_INT,
            }, {
              name: 'FirstName',
              type: glue.Schema.STRING,
            }, {
              name: 'LastName',
              type: glue.Schema.STRING,
            }, {
              name: 'ShortTVName',
              type: glue.Schema.STRING,
            }, {
              name: 'Team',
              type: glue.Schema.STRING,
            }, {
              name: 'NOC',
              type: glue.Schema.STRING,
            }, {
              name: 'Status',
              type: glue.Schema.STRING,
            }, {
              name: 'Laps',
              type: glue.Schema.INTEGER,
            }])),
          comment: 'Nested object with structure',
        }],
      partitionKeys: [{
        name: 'date_part',
        type: glue.Schema.STRING,
      }, {
        name: 'race_part',
        type: glue.Schema.STRING,
      }],
      dataFormat: glue.DataFormat.JSON,
    });

    // Table with the raw classification
    tableName = 'Classification';
    new glue.Table(this, tableName, {
      database: glueDBraw,
      tableName: tableName.toLowerCase(),
      bucket: props.s3raw,
      s3Prefix: `batch_data/${tableName}`,
      columns: [
        {
          name: 'Message',
          type: glue.Schema.STRING,
        }, {
          name: 'SeasonID',
          type: glue.Schema.INTEGER,
        }, {
          name: 'EventID',
          type: glue.Schema.INTEGER,
        }, {
          name: 'Gender',
          type: glue.Schema.STRING,
        }, {
          name: 'League',
          type: glue.Schema.STRING,
        }, {
          name: 'RaceType',
          type: glue.Schema.STRING,
        }, {
          name: 'State',
          type: glue.Schema.STRING,
        }, {
          name: 'TimeStamp',
          type: glue.Schema.STRING,
        }, {
          name: 'Results',
          type: glue.Schema.array(glue.Schema.struct([
            {
              name: 'Rank',
              type: glue.Schema.INTEGER,
            }, {
              name: 'Bib',
              type: glue.Schema.STRING,
            }, {
              name: 'UCIID',
              type: glue.Schema.BIG_INT,
            }, {
              name: 'FirstName',
              type: glue.Schema.STRING,
            }, {
              name: 'LastName',
              type: glue.Schema.STRING,
            }, {
              name: 'ShortTVName',
              type: glue.Schema.STRING,
            }, {
              name: 'Team',
              type: glue.Schema.STRING,
            }, {
              name: 'NOC',
              type: glue.Schema.STRING,
            }, {
              name: 'Status',
              type: glue.Schema.STRING,
            }, {
              name: 'Points',
              type: glue.Schema.INTEGER,
            }])),
          comment: 'Nested object with structure',
        }],
      partitionKeys: [{
        name: 'date_part',
        type: glue.Schema.STRING,
      }, {
        name: 'race_part',
        type: glue.Schema.STRING,
      }],
      dataFormat: glue.DataFormat.JSON,
    });

    // Table with the raw stream data
    tableName = 'stream_data';
    new glue.Table(this, tableName, {
      database: glueDBraw,
      tableName,
      bucket: props.s3raw,
      s3Prefix: tableName,
      columns: [
        {
          name: 'Message',
          type: glue.Schema.STRING,
        }, {
          name: 'TimeStamp',
          type: glue.Schema.STRING,
        }, {
          name: 'SeasonID',
          type: glue.Schema.INTEGER,
        }, {
          name: 'EventID',
          type: glue.Schema.INTEGER,
        }, {
          name: 'RaceID',
          type: glue.Schema.INTEGER,
        }, {
          name: 'Captures',
          type: glue.Schema.array(glue.Schema.struct([{
            name: 'TimeStamp',
            type: glue.Schema.STRING,
          }, {
            name: 'Bib',
            type: glue.Schema.STRING,
          }, {
            name: 'UCIID',
            type: glue.Schema.BIG_INT,
          }, {
            name: 'Rank',
            type: glue.Schema.INTEGER,
          }, {
            name: 'State',
            type: glue.Schema.STRING,
          }, {
            name: 'Distance',
            type: glue.Schema.DOUBLE,
          }, {
            name: 'DistanceProj',
            type: glue.Schema.DOUBLE,
          }, {
            name: 'Speed',
            type: glue.Schema.DOUBLE,
          }, {
            name: 'SpeedMax',
            type: glue.Schema.DOUBLE,
          }, {
            name: 'SpeedAvg',
            type: glue.Schema.DOUBLE,
          }, {
            name: 'DistanceFirst',
            type: glue.Schema.DOUBLE,
          }, {
            name: 'DistanceNext',
            type: glue.Schema.DOUBLE,
          }, {
            name: 'Acc',
            type: glue.Schema.DOUBLE,
          }, {
            name: 'Pos',
            type: glue.Schema.struct([{
              name: 'Lat',
              type: glue.Schema.DOUBLE,
            }, {
              name: 'Lng',
              type: glue.Schema.DOUBLE,
            }]),
          }, {
            name: 'Heartrate',
            type: glue.Schema.INTEGER,
          }, {
            name: 'Cadency',
            type: glue.Schema.INTEGER,
          }, {
            name: 'Power',
            type: glue.Schema.INTEGER,
          }])),
          comment: 'Nested object with structure',
        }, {
          name: 'LapsToGo',
          type: glue.Schema.INTEGER,
        }, {
          name: 'DistanceToGo',
          type: glue.Schema.INTEGER,
        }, {
          name: 'RaceName',
          type: glue.Schema.STRING,
        }, {
          name: 'Bib',
          type: glue.Schema.STRING,
        }, {
          name: 'UCIID',
          type: glue.Schema.BIG_INT,
        }, {
          name: 'FirstName',
          type: glue.Schema.STRING,
        }, {
          name: 'LastName',
          type: glue.Schema.STRING,
        }, {
          name: 'ShortTVName',
          type: glue.Schema.STRING,
        }, {
          name: 'Team',
          type: glue.Schema.STRING,
        }, {
          name: 'NOC',
          type: glue.Schema.STRING,
        }, {
          name: 'RaceTime',
          type: glue.Schema.DOUBLE,
        }, {
          name: 'RaceSpeed',
          type: glue.Schema.DOUBLE,
        }],
      partitionKeys: [
        {
          name: 'year',
          type: glue.Schema.STRING,
        }, {
          name: 'month',
          type: glue.Schema.STRING,
        }, {
          name: 'day',
          type: glue.Schema.STRING,
        }],
      dataFormat: glue.DataFormat.JSON,
    });

    // Table with the raw UCI championship historic results data
    tableName = 'UCIChampionshipHistoricResults';
    new glue.Table(this, tableName, {
      database: glueDBraw,
      tableName: tableName.toLowerCase(),
      bucket: props.s3raw,
      s3Prefix: `batch_data/${tableName}`,
      columns: [
        {
          name: 'ID',
          type: glue.Schema.STRING,//glue.Schema.BIG_INT,
        }, {
          name: 'Competition',
          type: glue.Schema.STRING,
        }, {
          name: 'Race',
          type: glue.Schema.STRING,
        }, {
          name: 'Event',
          type: glue.Schema.STRING,
        }, {
          name: 'UCIID',
          type: glue.Schema.STRING,//glue.Schema.BIG_INT,
        }, {
          name: 'Name',
          type: glue.Schema.STRING,
        }, {
          name: 'Rank',
          type: glue.Schema.STRING,//glue.Schema.BIG_INT,
        }, {
          name: 'IRM',
          type: glue.Schema.STRING,
        }, {
          name: 'Result',
          type: glue.Schema.STRING,
        }, {
          name: 'UnkFirstName',
          type: glue.Schema.STRING,
        }, {
          name: 'UnkLastName',
          type: glue.Schema.STRING,
        }, {
          name: 'UnkName',
          type: glue.Schema.STRING,
        }, {
          name: 'UnkUCIID',
          type: glue.Schema.STRING,//glue.Schema.BIG_INT,
        }, {
          name: 'UnkGenderCode',
          type: glue.Schema.STRING,
        }, {
          name: 'UnkIOCCode',
          type: glue.Schema.STRING,
        }, {
          name: 'UnkContinentCode',
          type: glue.Schema.STRING,
        }, {
          name: 'Age',
          type: glue.Schema.STRING,//glue.Schema.BIG_INT,
        }, {
          name: 'CompetitionName',
          type: glue.Schema.STRING,
        }, {
          name: 'CompetitionAge',
          type: glue.Schema.STRING,//glue.Schema.BIG_INT,
        }, {
          name: 'CompetitionContinent',
          type: glue.Schema.STRING,
        }, {
          name: 'CompetitionEndDate',
          type: glue.Schema.STRING,//glue.Schema.TIMESTAMP
        }, {
          name: 'CompetitionId',
          type: glue.Schema.STRING,//glue.Schema.BIG_INT,
        }, {
          name: 'CompetitionCountry',
          type: glue.Schema.STRING,//glue.Schema.STRING,
        }, {
          name: 'CompetitionIsocode2',
          type: glue.Schema.STRING,
        }, {
          name: 'CompetitionRiderCategory',
          type: glue.Schema.STRING,
        }, {
          name: 'CompetitionStartDate',
          type: glue.Schema.STRING,//glue.Schema.TIMESTAMP
        }, {
          name: 'ContactDetail',
          type: glue.Schema.STRING,
        }, {
          name: 'Continent',
          type: glue.Schema.STRING,
        }, {
          name: 'Discipline',
          type: glue.Schema.STRING,
        }, {
          name: 'EventType',
          type: glue.Schema.STRING,
        }, {
          name: 'EventTypeName',
          type: glue.Schema.STRING,
        }, {
          name: 'FirstName',
          type: glue.Schema.STRING,
        }, {
          name: 'Gender',
          type: glue.Schema.STRING,
        }, {
          name: 'Country',
          type: glue.Schema.STRING,
        }, {
          name: 'LastName',
          type: glue.Schema.STRING,
        }, {
          name: 'LocationName',
          type: glue.Schema.STRING,
        }, {
          name: 'ParaSportClass',
          type: glue.Schema.STRING,
        }, {
          name: 'RaceName',
          type: glue.Schema.STRING,
        }, {
          name: 'RaceCategory',
          type: glue.Schema.STRING,
        }, {
          name: 'RaceClass',
          type: glue.Schema.STRING,
        }, {
          name: 'RaceEndDate',
          type: glue.Schema.STRING,//glue.Schema.TIMESTAMP
        }, {
          name: 'RaceGender',
          type: glue.Schema.STRING,
        }, {
          name: 'RaceRaceCode',
          type: glue.Schema.STRING,
        }, {
          name: 'RaceStartDate',
          type: glue.Schema.STRING,//glue.Schema.TIMESTAMP
        }, {
          name: 'RaceType',
          type: glue.Schema.STRING,
        }, {
          name: 'Code',
          type: glue.Schema.STRING,
        }, {
          name: 'ResultTeamType',
          type: glue.Schema.STRING,
        }, {
          name: 'ResultType',
          type: glue.Schema.STRING,
        }, {
          name: 'RiderCategory',
          type: glue.Schema.STRING,
        }, {
          name: 'SeasonID',
          type: glue.Schema.STRING,//glue.Schema.BIG_INT,
        }, {
          name: 'Season',
          type: glue.Schema.STRING,//glue.Schema.BIG_INT,
        }, {
          name: 'SortOrder',
          type: glue.Schema.STRING,//glue.Schema.BIG_INT,
        }, {
          name: 'TeamCode',
          type: glue.Schema.STRING,
        }, {
          name: 'TeamId',
          type: glue.Schema.STRING,//glue.Schema.INTEGER,
        }, {
          name: 'ParentTeam',
          type: glue.Schema.STRING,//glue.Schema.INTEGER,
        }, {
          name: 'TeamName',
          type: glue.Schema.STRING,
        }, {
          name: 'TeamType',
          type: glue.Schema.STRING,
        }, {
          name: 'BirthDate',
          type: glue.Schema.STRING,//glue.Schema.TIMESTAMP
        }, {
          name: 'ClassificationStatus',
          type: glue.Schema.STRING,
        }, {
          name: 'CompetitionClassCode',
          type: glue.Schema.STRING,
        }, {
          name: 'CompetitionClassPresentation',
          type: glue.Schema.STRING,
        }, {
          name: 'RaceClassPresentation',
          type: glue.Schema.STRING,
        }],
      dataFormat: glue.DataFormat.CSV,
    });

    /*
    // To be used for integration test.
    // After Glue jobs run, the resulting Schemas should be the following
    tableName = 'rider_performance';
    new glue.Table(this, tableName, {
      database: glueDBstage,
      tableName,
      bucket: props.s3staging,
      s3Prefix: (`${tableName}/`),
      columns: [{
        name: 'ts',
        type: glue.Schema.TIMESTAMP,
      }, {
        name: 'year',
        type: glue.Schema.STRING,
      }, {
        name: 'month',
        type: glue.Schema.STRING,
      }, {
        name: 'day',
        type: glue.Schema.STRING,
      }, {
        name: 'ts_mes',
        type: glue.Schema.TIMESTAMP,
      }, {
        name: 'bib',
        type: glue.Schema.STRING,
      }, {
        name: 'heartrate',
        type: glue.Schema.DOUBLE,
      }, {
        name: 'cadence',
        type: glue.Schema.DOUBLE,
      }, {
        name: 'power',
        type: glue.Schema.DOUBLE,
      }],
      partitionKeys: [{
        name: 'race_id',
        type: glue.Schema.INTEGER,
      }],
      dataFormat: glue.DataFormat.JSON,
    });

    tableName = 'rider_kinematics';
    new glue.Table(this, tableName, {
      database: glueDBstage,
      tableName,
      bucket: props.s3staging,
      s3Prefix: (`${tableName}/`),
      columns: [{
        name: 'ts',
        type: glue.Schema.TIMESTAMP,
      }, {
        name: 'year',
        type: glue.Schema.STRING,
      }, {
        name: 'month',
        type: glue.Schema.STRING,
      }, {
        name: 'day',
        type: glue.Schema.STRING,
      }, {
        name: 'ts_mes',
        type: glue.Schema.TIMESTAMP,
      }, {
        name: 'bib',
        type: glue.Schema.STRING,
      }, {
        name: 'rank',
        type: glue.Schema.DOUBLE,
      }, {
        name: 'state',
        type: glue.Schema.STRING,
      }, {
        name: 'distance',
        type: glue.Schema.DOUBLE,
      }, {
        name: 'distance_proj',
        type: glue.Schema.DOUBLE,
      }, {
        name: 'speed',
        type: glue.Schema.DOUBLE,
      }, {
        name: 'speed_max',
        type: glue.Schema.DOUBLE,
      }, {
        name: 'speed_avg',
        type: glue.Schema.DOUBLE,
      }, {
        name: 'distance_first',
        type: glue.Schema.DOUBLE,
      }, {
        name: 'distance_next',
        type: glue.Schema.DOUBLE,
      }, {
        name: 'acc',
        type: glue.Schema.DOUBLE,
      }, {
        name: 'lat',
        type: glue.Schema.DOUBLE,
      }, {
        name: 'lng',
        type: glue.Schema.DOUBLE,
      }],
      partitionKeys: [{
        name: 'race_id',
        type: glue.Schema.INTEGER,
      }],
      dataFormat: glue.DataFormat.JSON,
    });

    tableName = 'race_list';
    new glue.Table(this, tableName, {
      database: glueDBstage,
      tableName,
      bucket: props.s3staging,
      s3Prefix: (`${tableName}/`),
      columns: [{
        name: 'ts',
        type: glue.Schema.TIMESTAMP,
      }, {
        name: 'date',
        type: glue.Schema.TIMESTAMP,
      }, {
        name: 'event_name',
        type: glue.Schema.STRING,
      }, {
        name: 'races_id',
        type: glue.Schema.BIG_INT,
      }, {
        name: 'races_type',
        type: glue.Schema.STRING,
      }, {
        name: 'races_laps',
        type: glue.Schema.BIG_INT,
      }, {
        name: 'races_distance',
        type: glue.Schema.BIG_INT,
      }, {
        name: 'races_race_name',
        type: glue.Schema.STRING,
      }, {
        name: 'races_start_time',
        type: glue.Schema.TIMESTAMP,
      }, {
        name: 'date_part',
        type: glue.Schema.STRING,
      }],
      dataFormat: glue.DataFormat.JSON,
    });

    tableName = 'race_start_list';
    new glue.Table(this, tableName, {
      database: glueDBstage,
      tableName,
      bucket: props.s3staging,
      s3Prefix: (`${tableName}/`),
      columns: [{
        name: 'ts',
        type: glue.Schema.TIMESTAMP,
      }, {
        name: 'race_id',
        type: glue.Schema.INTEGER,
      }, {
        name: 'race_type',
        type: glue.Schema.STRING,
      }, {
        name: 'info',
        type: glue.Schema.STRING,
      }, {
        name: 'race_name',
        type: glue.Schema.STRING,
      }, {
        name: 'laps',
        type: glue.Schema.INTEGER,
      }, {
        name: 'distance',
        type: glue.Schema.INTEGER,
      }, {
        name: 'startlist_bib',
        type: glue.Schema.BIG_INT,
      }, {
        name: 'startlist_name',
        type: glue.Schema.STRING,
      }, {
        name: 'startlist_team',
        type: glue.Schema.STRING,
      }, {
        name: 'startlist_nat',
        type: glue.Schema.STRING,
      }, {
        name: 'startlist_state',
        type: glue.Schema.STRING,
      }, {
        name: 'startlist_start_position',
        type: glue.Schema.BIG_INT,
      }, {
        name: 'startlist_start_lane',
        type: glue.Schema.BIG_INT,
      }, {
        name: 'date_part',
        type: glue.Schema.STRING,
      }],
      partitionKeys: [{
        name: 'race_part',
        type: glue.Schema.STRING,
      }],
      dataFormat: glue.DataFormat.JSON,
    });

    tableName = 'race_results';
    new glue.Table(this, tableName, {
      database: glueDBstage,
      tableName,
      bucket: props.s3staging,
      s3Prefix: (`${tableName}/`),
      columns: [{
        name: 'ts',
        type: glue.Schema.TIMESTAMP,
      }, {
        name: 'race_id',
        type: glue.Schema.INTEGER,
      }, {
        name: 'type',
        type: glue.Schema.STRING,
      }, {
        name: 'info',
        type: glue.Schema.STRING,
      }, {
        name: 'state',
        type: glue.Schema.STRING,
      }, {
        name: 'race_name',
        type: glue.Schema.STRING,
      }, {
        name: 'laps',
        type: glue.Schema.INTEGER,
      }, {
        name: 'distance',
        type: glue.Schema.INTEGER,
      }, {
        name: 'race_speed',
        type: glue.Schema.DOUBLE,
      }, {
        name: 'race_speed_unit',
        type: glue.Schema.STRING,
      }, {
        name: 'race_duration',
        type: glue.Schema.DOUBLE,
      }, {
        name: 'results_rank',
        type: glue.Schema.INTEGER,
      }, {
        name: 'results_bib',
        type: glue.Schema.BIG_INT,
      }, {
        name: 'results_name',
        type: glue.Schema.STRING,
      }, {
        name: 'results_team',
        type: glue.Schema.STRING,
      }, {
        name: 'results_nat',
        type: glue.Schema.STRING,
      }, {
        name: 'results_state',
        type: glue.Schema.STRING,
      }, {
        name: 'results_laps',
        type: glue.Schema.BIG_INT,
      }, {
        name: 'date_part',
        type: glue.Schema.STRING,
      }],
      partitionKeys: [{
        name: 'race_part',
        type: glue.Schema.STRING,
      }],
      dataFormat: glue.DataFormat.JSON,
    });

    tableName = 'rider_stream_data';
    new glue.Table(this, tableName, {
      database: glueDBpoststage,
      tableName,
      bucket: props.s3staging,
      s3Prefix: (`${tableName}/`),
      columns: [{
        name: 'ts',
        type: glue.Schema.TIMESTAMP,
      }, {
        name: 'year',
        type: glue.Schema.STRING,
      }, {
        name: 'month',
        type: glue.Schema.STRING,
      }, {
        name: 'day',
        type: glue.Schema.STRING,
      }, {
        name: 'ts_mes',
        type: glue.Schema.TIMESTAMP,
      }, {
        name: 'bib',
        type: glue.Schema.BIG_INT,
      }, {
        name: 'rank',
        type: glue.Schema.INTEGER,
      }, {
        name: 'state',
        type: glue.Schema.STRING,
      }, {
        name: 'distance',
        type: glue.Schema.DOUBLE,
      }, {
        name: 'distance_proj',
        type: glue.Schema.DOUBLE,
      }, {
        name: 'speed',
        type: glue.Schema.DOUBLE,
      }, {
        name: 'speed_max',
        type: glue.Schema.DOUBLE,
      }, {
        name: 'speed_avg',
        type: glue.Schema.DOUBLE,
      }, {
        name: 'distance_first',
        type: glue.Schema.DOUBLE,
      }, {
        name: 'distance_next',
        type: glue.Schema.DOUBLE,
      }, {
        name: 'acc',
        type: glue.Schema.DOUBLE,
      }, {
        name: 'lat',
        type: glue.Schema.DOUBLE,
      }, {
        name: 'lng',
        type: glue.Schema.DOUBLE,
      }, {
        name: 'heartrate',
        type: glue.Schema.DOUBLE,
      }, {
        name: 'cadence',
        type: glue.Schema.DOUBLE,
      }, {
        name: 'power',
        type: glue.Schema.DOUBLE,
      }],
      partitionKeys: [{
        name: 'race_id',
        type: glue.Schema.INTEGER,
      }],
      dataFormat: glue.DataFormat.JSON,
    });
    */

    /*
    ####################
    # Glue Jobs Role Definition
    ####################
    */
    // Create jobs for tables
    // Define a role and a policy for glue services
    const glueJobRole = new iam.Role(this, 'glueJobRole', {
      assumedBy: new iam.ServicePrincipal('glue.amazonaws.com'),
    });

    // Grant permissions for future script execution
    glueJobRole.addManagedPolicy(
      iam.ManagedPolicy.fromAwsManagedPolicyName('service-role/AWSGlueServiceRole')
    );
    // Grant permissions for S3
    //glueJobRole.addManagedPolicy(
    //  iam.ManagedPolicy.fromAwsManagedPolicyName('AmazonS3FullAccess')
    //);
    // with reference to https://docs.aws.amazon.com/glue/latest/dg/create-service-policy.html
    glueJobRole.addToPolicy(new iam.PolicyStatement({
      resources: [
        'arn:aws:s3:::*'
      ],
      actions: [
        's3:GetBucketLocation',
        's3:ListBucket',
        's3:ListAllMyBuckets',
        's3:GetBucketAcl',
        's3:GetObject',
        's3:PutObject',
        's3:DeleteObject'
      ],
    }));
    // Grant permissions for Athena
    glueJobRole.addManagedPolicy(
      iam.ManagedPolicy.fromAwsManagedPolicyName('AmazonAthenaFullAccess')
    );

    // Grant permissions for DynamoDB
    glueJobRole.addManagedPolicy(
      iam.ManagedPolicy.fromAwsManagedPolicyName('AmazonDynamoDBFullAccess')
    );

    // Assigne the role to the S3 bucket
    props.s3Temp.grantRead(glueJobRole);

    /*
    ####################
    # Glue Jobs Defintion
    ####################
    */
    // Create a Job for getting data from raw and put to staging
    jobName = 'raw_to_staging_racelist';
    new glue.CfnJob(this, jobName, {
      role: glueJobRole.roleArn,
      glueVersion: '1.0',
      name: envSpecific(jobName, true),
      description: 'Job for importing data from the raw to staging',
      maxCapacity: 1,
      command: {
        name: 'pythonshell',
        pythonVersion: '3',
        scriptLocation: (`s3://${props.s3Temp.bucketName}/glue_jobs/scripts/raw_to_staging/${jobName}.py`),
      },
      // Set the additional dll
      defaultArguments: {
        '--extra-py-files': (`s3://${props.s3Temp.bucketName}/glue_jobs/glue_helper_libraries/dist/glue_python_shell_helper_libraries-0.1-py3-none-any.whl`),
        '--src_database': glueDBraw.databaseName,
        '--src_table': 'raceslist',
        '--target_bucket': props.s3staging.bucketName,
        '--target_database': glueDBstage.databaseName,
        '--target_table': 'raceslist',
      },
    });

    jobName = 'raw_to_staging_racestartlist';
    new glue.CfnJob(this, jobName, {
      role: glueJobRole.roleArn,
      glueVersion: '1.0',
      name: envSpecific(jobName, true),
      description: 'Job for importing data from the raw to staging',
      maxCapacity: 1,
      command: {
        name: 'pythonshell',
        pythonVersion: '3',
        scriptLocation: (`s3://${props.s3Temp.bucketName}/glue_jobs/scripts/raw_to_staging/${jobName}.py`),
      },
      // Set the additional dll
      defaultArguments: {
        '--extra-py-files': (`s3://${props.s3Temp.bucketName}/glue_jobs/glue_helper_libraries/dist/glue_python_shell_helper_libraries-0.1-py3-none-any.whl`),
        '--src_database': glueDBraw.databaseName,
        '--src_table': 'racestartlist',
        '--target_bucket': props.s3staging.bucketName,
        '--target_database': glueDBstage.databaseName,
        '--target_table': 'racestartlist',
      },
    });

    jobName = 'raw_to_staging_raceresults';
    new glue.CfnJob(this, jobName, {
      role: glueJobRole.roleArn,
      glueVersion: '1.0',
      name: envSpecific(jobName, true),
      description: 'Job for importing data from the raw to staging',
      maxCapacity: 1,
      command: {
        name: 'pythonshell',
        pythonVersion: '3',
        scriptLocation: (`s3://${props.s3Temp.bucketName}/glue_jobs/scripts/raw_to_staging/${jobName}.py`),
      },
      // Set the additional dll
      defaultArguments: {
        '--extra-py-files': (`s3://${props.s3Temp.bucketName}/glue_jobs/glue_helper_libraries/dist/glue_python_shell_helper_libraries-0.1-py3-none-any.whl`),
        '--src_database': glueDBraw.databaseName,
        '--src_table': 'raceresults',
        '--target_bucket': props.s3staging.bucketName,
        '--target_database': glueDBstage.databaseName,
        '--target_table': 'raceresults',
      },
    });

    jobName = 'raw_to_staging_stream';
    new glue.CfnJob(this, jobName, {
      role: glueJobRole.roleArn,
      glueVersion: '1.0',
      name: envSpecific(jobName, true),
      description: 'Job for importing data from the raw to staging',
      maxCapacity: 1,
      command: {
        name: 'pythonshell',
        pythonVersion: '3',
        scriptLocation: (`s3://${props.s3Temp.bucketName}/glue_jobs/scripts/raw_to_staging/${jobName}.py`),
      },
      // Set the additional dll
      defaultArguments: {
        '--extra-py-files': (`s3://${props.s3Temp.bucketName}/glue_jobs/glue_helper_libraries/dist/glue_python_shell_helper_libraries-0.1-py3-none-any.whl`),
        '--src_database': glueDBraw.databaseName,
        '--src_table': 'stream_data',
        '--target_bucket': props.s3staging.bucketName,
        '--target_database': glueDBstage.databaseName,
        '--target_table_starttime': 'starttime',
        '--target_table_lapcounter': 'lapcounter',
        '--target_table_ridereliminated': 'ridereliminated',
        '--target_table_finishtime': 'finishtime',
        '--target_table_kinematics': 'rider_kinematics',
        '--target_table_performance': 'rider_performance',
      },
    });

    jobName = 'raw_to_staging_ucichampionshiphistoricresults';
    new glue.CfnJob(this, jobName, {
      role: glueJobRole.roleArn,
      glueVersion: '1.0',
      name: envSpecific(jobName, true),
      description: 'Job for importing data from the raw to staging',
      maxCapacity: 1,
      command: {
        name: 'pythonshell',
        pythonVersion: '3',
        scriptLocation: (`s3://${props.s3Temp.bucketName}/glue_jobs/scripts/raw_to_staging/${jobName}.py`),
      },
      // Set the additional dll
      defaultArguments: {
        '--extra-py-files': (`s3://${props.s3Temp.bucketName}/glue_jobs/glue_helper_libraries/dist/glue_python_shell_helper_libraries-0.1-py3-none-any.whl`),
        '--src_database': glueDBraw.databaseName,
        '--src_table': 'ucichampionshiphistoricresults',
        '--target_bucket': props.s3staging.bucketName,
        '--target_database': glueDBstage.databaseName,
        '--target_table': 'ucichampionshiphistoricresults',
      },
    });

    jobName = 'staging_to_poststaging_stream';
    new glue.CfnJob(this, jobName, {
      role: glueJobRole.roleArn,
      glueVersion: '1.0',
      name: envSpecific(jobName, true),
      description: 'Job for importing data from the raw to staging',
      maxCapacity: 1,
      command: {
        name: 'pythonshell',
        pythonVersion: '3',
        scriptLocation: (`s3://${props.s3Temp.bucketName}/glue_jobs/scripts/staging_to_poststaging/${jobName}.py`),
      },
      // Set the additional dll
      defaultArguments: {
        '--extra-py-files': (`s3://${props.s3Temp.bucketName}/glue_jobs/glue_helper_libraries/dist/glue_python_shell_helper_libraries-0.1-py3-none-any.whl`),
        '--src_database': glueDBstage.databaseName,
        '--src_table_kinematics': 'rider_kinematics',
        '--src_table_performance': 'rider_performance',
        '--target_bucket': props.s3poststaging.bucketName,
        '--target_database': glueDBpoststage.databaseName,
        '--target_table': 'rider_stream_data',
      },
    });

    jobName = 'staging_to_poststaging_elocalibration';
    new glue.CfnJob(this, jobName, {
      role: glueJobRole.roleArn,
      glueVersion: '1.0',
      name: envSpecific(jobName, true),
      description: 'Job for importing data from the raw to staging',
      maxCapacity: 1,
      command: {
        name: 'pythonshell',
        pythonVersion: '3',
        scriptLocation: (`s3://${props.s3Temp.bucketName}/glue_jobs/scripts/staging_to_poststaging/${jobName}.py`),
      },
      // Set the additional dll
      defaultArguments: {
        '--extra-py-files': (`s3://${props.s3Temp.bucketName}/glue_jobs/glue_helper_libraries/dist/glue_python_shell_helper_libraries-0.1-py3-none-any.whl`),
        '--src_database': glueDBstage.databaseName,
        '--src_table_historicresults': 'ucichampionshiphistoricresults',
        '--src_table_raceresults': 'raceresults',
        '--target_bucket': props.s3poststaging.bucketName,
        '--target_database': glueDBpoststage.databaseName,
        '--target_table': 'riders_elos',
        '--ddb_table': props.ddbTableStatic.tableName,
        '--USE_TRIMARAN_DATA': 'False', // bool: 'True', 'False'
        '--PROXY_ELIMINATION_WITH_OMNIUM': 'True', // bool: 'True', 'False'
        '--RACE_LEAGUE': 'sprint', // string: 'endurance', 'sprint'
        '--LOGGER_LEVEL': 'DEBUG', // string: 'DEBUG', 'INFO', 'WARNING', 'ERROR'
      },
    });
  }
}
