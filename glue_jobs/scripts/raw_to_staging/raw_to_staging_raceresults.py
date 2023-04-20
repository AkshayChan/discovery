import boto3
import pandas as pd
import awswrangler as wr
import sys
from awsglue.utils import getResolvedOptions

args = getResolvedOptions(sys.argv, ['src_database', 'src_table', 'target_bucket', 'target_database', 'target_table'])
src_database = args['src_database'] #'eurosport_cycling_raw'
src_table = args['src_table'] #'race_results'
target_bucket = args['target_bucket'] #'eurosport-cycling-staging-dev'
target_database = args['target_database'] #'eurosport_cycling_staging'
target_table = args['target_table'] #race_results


session = boto3.Session(region_name='eu-west-1')
sql_query = lambda table_name: f"SELECT * FROM {table_name}"
wr.athena.repair_table(table=src_table, database=src_database, boto3_session=session)
df_raw = wr.athena.read_sql_query(sql_query(src_table), database=src_database, boto3_session=session)

list_column = 'results'
# list of results to rows
df_raw = df_raw.explode(list_column)
# result dictionaries to columns
df_raw = pd.concat([df_raw.drop([list_column], axis=1), df_raw[list_column].apply(pd.Series).add_prefix(list_column+'_')], axis=1)
# ts to datetime format
df_raw.timestamp = pd.to_datetime(df_raw.timestamp, format='%Y-%m-%dT%H:%M:%S.%fZ')

result_column_list = ['message', 'seasonid', 'eventid', 'raceid', 'racetype', 'gender', 'league', 'heat', 'totalheats', 'round', 'totalrounds', 'state', 'racename', 'laps', 'distance', 'racetime', 'racespeed', 'timestamp',
'results_rank', 'results_bib', 'results_uciid', 'results_firstname', 'results_lastname', 'results_shorttvname', 'results_team', 'results_noc', 'results_status', 'results_laps', 'date_part', 'race_part']

df_raw = df_raw[result_column_list]

# write to s3
wr.s3.to_parquet(
    df=df_raw,
    path=f's3://{target_bucket}/{target_table}/',
    dataset=True,
    database=target_database,
    table=target_table,
    mode='append',
    partition_cols=['race_part'],
    boto3_session=session
)