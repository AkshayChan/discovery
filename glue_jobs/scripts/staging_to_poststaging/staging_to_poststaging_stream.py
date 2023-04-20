import boto3
import pandas as pd
import awswrangler as wr
import sys
from awsglue.utils import getResolvedOptions

args = getResolvedOptions(sys.argv, ['src_database', 'src_table_kinematics', 'src_table_performance', 'target_bucket', 'target_database', 'target_table'])
src_database = args['src_database'] #'eurosport_cycling_staging'
src_table_kinematics = args['src_table_kinematics'] #'rider_kinematics'
src_table_performance = args['src_table_performance'] #'rider_performance'
target_bucket = args['target_bucket'] #'eurosport-cycling-post-staging-dev'
target_database = args['target_database'] #'eurosport_cycling_poststaging'
target_table = args['target_table'] #rider_stream_data

calibrate_mergeasof_tolerance = False

session = boto3.Session(region_name='eu-west-1')
sql_query = lambda table_name: f"SELECT * FROM {table_name}"
df_kinematics = wr.athena.read_sql_query(sql_query(src_table_kinematics), database=src_database, boto3_session=session)
df_performance = wr.athena.read_sql_query(sql_query(src_table_performance), database=src_database, boto3_session=session)
df_kinematics = df_kinematics.sort_values(['timestamp'])
df_performance = df_performance.sort_values(['timestamp'])

if calibrate_mergeasof_tolerance:
    # based on the histogram the cutoff merge_asof tolerance should be 2 seconds, as most max distances between 2 timestamps are smaller than 2 seconds
    df_performance = df_performance.reset_index()
    df_kinematics = df_kinematics.reset_index()
    df_merge = pd.merge_asof(df_kinematics, df_performance[['index', 'timestamp', 'heartrate', 'cadency', 'power', 'raceid', 'bib', 'uciid']], 
              on='timestamp', by=['raceid', 'bib', 'uciid'], tolerance=pd.Timedelta('4s'), direction='backward')
    # merge timestamp from df_performance to validate timestmap diff
    df_merge = df_merge.merge(df_performance[['index', 'timestamp']], left_on='index_y', right_on='index', how='left')
    df_merge['timestamp_diff'] = df_merge.timestamp_x - df_merge.timestamp_y
    display(df_merge[df_merge.timestamp_diff.isnull()])
    df_merge.timestamp_diff.dt.total_seconds().hist(bins=30)
else:
    df_merge = pd.merge_asof(df_kinematics, df_performance[['timestamp', 'heartrate', 'cadency', 'power', 'raceid', 'bib', 'uciid']], 
              on='timestamp', by=['raceid', 'bib', 'uciid'], tolerance=pd.Timedelta('2s'), direction='backward')
    
    # write to s3
    wr.s3.to_parquet(
        df=df_merge,
        path=f's3://{target_bucket}/{target_table}/',
        dataset=True,
        database=target_database,
        table=target_table,
        mode='append',
        partition_cols=['raceid'],
        boto3_session=session
    )