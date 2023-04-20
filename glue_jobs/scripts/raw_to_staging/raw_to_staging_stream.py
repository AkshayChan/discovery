import boto3
import pandas as pd
import awswrangler as wr
import sys
from awsglue.utils import getResolvedOptions

def split_stream(df, message_name, message_cols, target_table, target_database, target_bucket, session):
    """
    Split the streamed data into their respective messages and write to Athena and S3.
    
    Arguments:
        df (pd.DataFrame): DataFrame containing the complete stream of all messages
        message_name (str): message_name to filter for in the DataFrame
        message_cols (list[str]): list of columns which are specific to this message_name
        target_table (str): target table to which to write the filtered DataFrame
        target_database (str): target database to which to write the filtered DataFrame
        target_bucket (str): target bucket to which to write the filtered DataFrame
        session (boto3.Session): boto3 Session to use for writing to Athena/S3
    """
    df_result = df[(df.message==message_name)][message_cols]
    if df_result.shape[1] < len(message_cols):
        missing_fields = list(set(message_cols) - set(df.columns))
        raise ValueError(f"Missing columns from message {message_name}: {', '.join(missing_fields)}. Expected to have columns {', '.join(message_cols)} in the message. ")
    # replace string capture in column names
    renamed_cols = [c.replace('captures_', '') if c!='captures_timestamp' else c for c in message_cols]
    df_result.columns = renamed_cols
    # write to s3
    wr.s3.to_parquet(
        df=df_result,
        path=f's3://{target_bucket}/{target_table}/',
        dataset=True,
        database=target_database,
        table=target_table,
        mode='append',
        partition_cols=['raceid'],
        boto3_session=session
    )
    
    return df_result

# read arguments
args = getResolvedOptions(sys.argv, ['src_database', 'src_table', 'target_bucket', 'target_database', 'target_table_starttime', 'target_table_lapcounter', 'target_table_ridereliminated', 'target_table_finishtime', 'target_table_kinematics', 'target_table_performance'])
src_database = args['src_database'] #'eurosport_cycling_raw'
src_table = args['src_table'] #'stream_data'
target_bucket = args['target_bucket'] #'eurosport-cycling-staging-dev'
target_database = args['target_database'] #'eurosport_cycling_staging'
target_table_starttime = args['target_table_starttime'] #'starttime'
target_table_lapcounter = args['target_table_lapcounter'] #'lapcounter'
target_table_ridereliminated = args['target_table_ridereliminated'] #'ridereliminated'
target_table_finishtime = args['target_table_finishtime'] #'finishtime'
target_table_kinematics = args['target_table_kinematics'] #'rider_kinematics'
target_table_performance = args['target_table_performance'] #'rider_performance'

# read data from src table
session = boto3.Session(region_name='eu-west-1')
sql_query = lambda table_name: f"SELECT * FROM {table_name}"
wr.athena.repair_table(table=src_table, database=src_database, boto3_session=session)
df_raw = wr.athena.read_sql_query(sql_query(src_table), database=src_database, boto3_session=session)

# list of captures to rows
df_raw = df_raw.explode('captures')
# captures dictionaries to columns
df_raw = pd.concat([df_raw.drop(['captures'], axis=1), df_raw.captures.apply(pd.Series).add_prefix('captures_')], axis=1)
# pos dictionaries to columns
df_raw = pd.concat([df_raw.drop(['captures_pos'], axis=1), df_raw.captures_pos.apply(pd.Series).add_prefix('captures_pos_')[['captures_pos_lat', 'captures_pos_lng']]], axis=1)
# timestamp to datetime format
df_raw.captures_timestamp = pd.to_datetime(df_raw.captures_timestamp, format='%Y-%m-%d %H:%M:%S.%f')
df_raw.timestamp = pd.to_datetime(df_raw.timestamp, format='%Y-%m-%dT%H:%M:%S.%fZ')

# split the stream into it's distinct message types
# all messages have these columns in common
base_cols = ['message', 'timestamp', 'seasonid', 'eventid', 'raceid']

# StartTime
message_cols = base_cols
split_stream(df_raw, 'StartTime', message_cols, target_table_starttime, target_database, target_bucket, session)

# LapCounter
message_cols = ['lapstogo', 'distancetogo']
message_cols = base_cols + message_cols
split_stream(df_raw, 'LapCounter', message_cols, target_table_lapcounter, target_database, target_bucket, session)

# RiderEliminated
message_cols = ['racename', 'bib', 'uciid', 'firstname', 'lastname', 'shorttvname', 'team', 'noc']
message_cols = base_cols + message_cols
split_stream(df_raw, 'RiderEliminated', message_cols, target_table_ridereliminated, target_database, target_bucket, session)

# FinishTime
message_cols = ['racetime', 'racespeed']
message_cols = base_cols + message_cols
split_stream(df_raw, 'FinishTime', message_cols, target_table_finishtime, target_database, target_bucket, session)

# LiveRidersTracking
message_cols = ['timestamp', 'bib', 'uciid', 'rank', 'state', 'distance', 'distanceproj', 'speed', 'speedmax', 'speedavg', 'distancefirst', 'distancenext', 'acc', 'pos_lat', 'pos_lng']
message_cols = ['captures_'+c for c in message_cols]
message_cols = base_cols + message_cols
split_stream(df_raw, 'LiveRidersTracking', message_cols, target_table_kinematics, target_database, target_bucket, session)

# LiveRidersData
message_cols = ['timestamp', 'bib', 'uciid', 'heartrate', 'cadency', 'power']
message_cols = ['captures_'+c for c in message_cols]
message_cols = base_cols + message_cols
split_stream(df_raw, 'LiveRidersData', message_cols, target_table_performance, target_database, target_bucket, session)
