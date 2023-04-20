import boto3
import pandas as pd
import numpy as np
import awswrangler as wr
import sys
from awsglue.utils import getResolvedOptions

args = getResolvedOptions(sys.argv, ['src_database', 'src_table', 'target_bucket', 'target_database', 'target_table'])
src_database = args['src_database'] #'dev_eurosport_cycling_raw'
src_table = args['src_table'] #'ucichampionshiphistoricresults'
target_bucket = args['target_bucket'] #'dev-eurosportcyclingstorage-staging391e4595-zxo54vzfmckl'
target_database = args['target_database'] #'dev_eurosport_cycling_staging'
target_table = args['target_table'] #ucichampionshiphistoricresults

#src_database = 'dev_eurosport_cycling_raw'
#src_table = 'ucichampionshiphistoricresults'
#target_bucket = 'dev-eurosportcyclingstorage-staging391e4595-zxo54vzfmckl'
#target_database = 'dev_eurosport_cycling_staging'
#target_table = 'ucichampionshiphistoricresults'

# read data from Athena
#session = boto3.Session(region_name='eu-west-1', profile_name='uci-tcl-aws-dev')
session = boto3.Session(region_name='eu-west-1')
sql_query = lambda table_name: f"SELECT * FROM {table_name}"
wr.athena.repair_table(table=src_table, database=src_database, boto3_session=session)
df_raw = wr.athena.read_sql_query(sql_query(src_table), database=src_database, boto3_session=session)

# transform rank and uciid types to integer
# fill in the unknown uciid, firstname, lastname, country with the unknown columns
df_raw['rank'] = pd.to_numeric(df_raw['rank'], errors='coerce').fillna(99).astype(int)
df_raw['uciid_numeric'] = pd.to_numeric(df_raw.uciid, errors='coerce')
df_raw.loc[df_raw.uciid_numeric.isnull(), 'uciid_numeric'] = pd.to_numeric(df_raw[df_raw.uciid_numeric.isnull()].unkuciid, errors='coerce')
df_raw.uciid_numeric = df_raw.uciid_numeric.astype(int)
df_raw.loc[df_raw.firstname=='', 'firstname'] = df_raw[df_raw.firstname==''].unkfirstname
df_raw.loc[df_raw.lastname=='', 'lastname'] = df_raw[df_raw.lastname==''].unklastname
df_raw.loc[df_raw.country=='', 'country'] = df_raw[df_raw.country==''].unkioccode

# Based on discussion with Gilles Peruzzi from 2021-10-01 all competitions containing an info about 6 days in the competition name should be removed
df_raw = df_raw[~df_raw.competitionname.isin(['Wooning Zesdaagse', '6 Giorni delle Rose - Fiorenzuola 2016', 'Zesdaagse van Rotterdam'])]

# determine race type name from race type
df_raw['race_type_name'] = df_raw.racetype.replace({'KT': '1 km Time Trial', 
                                                    'EL': 'Elimination Race', 
                                                    'IP': 'Individual Pursuit', 
                                                    'KE': 'Keirin',
                                                    'MA': 'Madison',
                                                    'OM': 'Omnium',
                                                    'PR': 'Points Race',
                                                    'SH': 'Scratch',
                                                    'SP': 'Sprint',
                                                    'TP': 'Team Pursuit',
                                                    'TS': 'Team Sprint',
                                                    'KT': 'Time Trial'})
df_result = pd.DataFrame()
df_result['message'] = df_raw.shape[0]*['HistoricResults']
df_result['seasonid'] = df_raw.seasonid.astype(int)
df_result['season'] = df_raw.season.astype(int)
df_result['eventid'] = df_raw.event
df_result['raceid'] = df_raw.race
df_result['racetype'] = df_raw.race_type_name
df_result['gender'] = df_raw.racegender
df_result['heat'] = 1
df_result['totalheats'] = 1
df_result['round'] = 1
df_result['totalrounds'] = 1
df_result['racename'] = df_raw.racename
df_result['timestamp'] = df_raw.raceenddate
df_result['results_rank'] = df_raw['rank']
df_result['results_uciid'] = df_raw.uciid_numeric
df_result['results_firstname'] = df_raw.firstname
df_result['results_lastname'] = df_raw.lastname
df_result['results_team'] = df_raw.teamcode
df_result['results_noc'] = df_raw.country
df_result['results_status'] = df_raw.irm
df_result['results_sortorder'] = df_raw.sortorder.astype(int)

# timestamp to datetime format
df_result.timestamp = pd.to_datetime(df_result.timestamp, format='%Y-%m-%d')

# write to s3
wr.s3.to_parquet(
    df=df_result,
    path=f's3://{target_bucket}/{target_table}/',
    dataset=True,
    database=target_database,
    table=target_table,
    mode='overwrite',#append
    boto3_session=session
)