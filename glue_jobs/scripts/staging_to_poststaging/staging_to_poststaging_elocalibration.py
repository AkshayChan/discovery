#!/usr/bin/env python
# coding: utf-8

# # Compute ELO Scores on historic race results

import trueskill
import pandas as pd
import numpy as np
import boto3
import awswrangler as wr
import time
import sys
import re
from elo_utils import *
import elo_utils.elo_helper as elo_helper
import elo_utils.race_preprocessing as preproc
import elo_utils.io as io
try:
    from awsglue.utils import getResolvedOptions
    glue_mode = True
except:
    glue_mode = False
    
if glue_mode:
    args = getResolvedOptions(sys.argv, ['src_database', 'src_table_historicresults', 'src_table_raceresults', 'target_bucket', 'target_database', 'target_table', 
                                         'ddb_table', 'USE_TRIMARAN_DATA', 'PROXY_ELIMINATION_WITH_OMNIUM', 'RACE_LEAGUE', 'LOGGER_LEVEL'])
    print(args)
    src_database = args['src_database'] #'dev_eurosport_cycling_staging'
    src_table_historicresults = args['src_table_historicresults'] #'ucichampionshiphistoricresults'
    src_table_raceresults = args['src_table_raceresults']#'raceresults'
    target_bucket = args['target_bucket'] #'dev-eurosportcyclingstorage-poststaging47f9ee0f-1rk89q4igkpwg'
    target_database = args['target_database'] #'dev_eurosport_cycling_poststaging'
    target_table = args['target_table'] #'eloscore'
    ddb_table = args['ddb_table'] #'dev-EurosportCyclingStorage-liveBCEC2B85-KBXYIVCRZ862'
    session = boto3.Session(region_name='eu-west-1')    
    
    # Configuration of parameters
    
    # use Trimaran race result data or not
    USE_TRIMARAN_DATA = args['USE_TRIMARAN_DATA'] # bool: True, False
    # use Omnium as proxy for Elimination (True) or not (False)
    PROXY_ELIMINATION_WITH_OMNIUM = args['PROXY_ELIMINATION_WITH_OMNIUM'] # bool: True, False
    RACE_LEAGUE = args['RACE_LEAGUE'] # string: 'endurance', 'sprint'
    if RACE_LEAGUE.lower() == 'sprint':
        RACE_TYPES = ['Sprint', 'Keirin']
    elif RACE_LEAGUE.lower() == 'endurance':
        RACE_TYPES = ['Scratch', 'Elimination Race'] 
    else:
        raise ValueError('Wrong value for RACE_LEAGUE. Expected values: "endurance" or "sprint"')
    # logger level
    LOGGER_LEVEL = args['LOGGER_LEVEL'] # string: 'DEBUG', 'INFO', 'WARNING', 'ERROR'
    # evalute Performance: 
    # True: do a train/test split and evalute performance on test data
    # False: no train/test split, using all data for training
    EVALUATE_PERFORMANCE = False
    EVALUATE_PERFORMANCE_TESTSEASON = [2021, 2020]
    # Number of Riders to evalute the winning rider on 
    EVALUATE_PERFORMANCE_N_BEST_RIDERS = 1 # int: 1, 2, 3, ...
    # Plot graphs of data for Data Analysis and debugging
    DEBUG_PLOTS = False
else:
    src_database = 'dev_eurosport_cycling_staging'
    src_table_historicresults = 'ucichampionshiphistoricresults'
    src_table_raceresults = 'raceresults'
    target_bucket = 'dev-eurosportcyclingstorage-poststaging47f9ee0f-1rk89q4igkpwg'
    target_database = 'dev_eurosport_cycling_poststaging'
    target_table = 'riders_elos'
    ddb_table = 'dev-EurosportCyclingStorage-liveBCEC2B85-KBXYIVCRZ862'
    session = boto3.Session(region_name='eu-west-1', profile_name='uci-tcl-aws-dev')
    
    # Configuration of parameters
    
    # use Omnium as proxy for Elimination (True) or not (False)
    PROXY_ELIMINATION_WITH_OMNIUM = True
    # evalute Performance: 
    # True: do a train/test split and evalute performance on test data
    # False: no train/test split, using all data for training
    EVALUATE_PERFORMANCE = True
    EVALUATE_PERFORMANCE_TESTSEASON = [2021, 2020]
    # Number of Riders to evalute the winning rider on 
    EVALUATE_PERFORMANCE_N_BEST_RIDERS = 1
    RACE_LEAGUE = 'sprint' # string: 'endurance', 'sprint'
    if RACE_LEAGUE.lower() == 'sprint':
        RACE_TYPES = ['Sprint', 'Keirin']
    elif RACE_LEAGUE.lower() == 'endurance':
        RACE_TYPES = ['Scratch', 'Elimination Race'] 
    else:
        raise ValueError('Wrong value for RACE_LEAGUE. Expected values: "endurance" or "sprint"')
    # logger level
    LOGGER_LEVEL = 'DEBUG'
    # Plot graphs of data for Data Analysis and debugging
    DEBUG_PLOTS = False
    USE_TRIMARAN_DATA = False
    
if DEBUG_PLOTS and not glue_mode:
    import matplotlib.pyplot as plt


# ## Read and clean raw data

logger.setLevel(LOGGER_LEVEL)
logger.info('')
logger.info('#### Reading input data from datalake ####')
# read historic race results
sql_query = lambda table_name: f"SELECT * FROM {table_name}"
wr.athena.repair_table(table=src_table_historicresults, database=src_database, boto3_session=session)
df_raw_uci = wr.athena.read_sql_query(sql_query(src_table_historicresults), database=src_database, boto3_session=session)
df_raw_uci.seasonid = df_raw_uci.season#.astype(int)
wr.athena.repair_table(table=src_table_raceresults, database=src_database, boto3_session=session)
df_raw_trimaran = wr.athena.read_sql_query(sql_query(src_table_raceresults), database=src_database, boto3_session=session)


# append historic UCI results with trimaran results 
common_cols = list(set(df_raw_trimaran.columns.tolist()).intersection(set(df_raw_uci.columns.tolist())))
common_cols_sorted = []
for col in df_raw_uci.columns:
    if col in common_cols:
        common_cols_sorted.append(col)
df_raw_uci = df_raw_uci[common_cols_sorted]
df_raw_trimaran = df_raw_trimaran[df_raw_trimaran.date_part>='2021-10-01'][common_cols_sorted]
if USE_TRIMARAN_DATA:
    df_raw = df_raw_uci.append(df_raw_trimaran)
else:
    df_raw = df_raw_uci.copy()


logger.info('')
logger.info('#### Clean raw data ####')
# drop riders with no UCIID
df_raw = df_raw.dropna(subset=['results_uciid'], how='any')
# extract race gender from race name
df_raw.gender = df_raw.gender.str.slice(0, 1)
# remove all non alphanumeric characters from string
df_raw.racename = df_raw.racename.str.strip().apply(lambda x: re.sub('[^0-9a-zA-Z]+', ' ', x))
# replace empty rempark with NAT
df_raw.results_status = df_raw.results_status.replace('', pd.NaT)
    
# drop duplicates
df_raw = df_raw.sort_values(GROUPBY_COLS+['results_uciid', 'results_rank'])
logger.warning(f"Duplicates on: {GROUPBY_COLS+['results_uciid']}")
logger.warning(df_raw[df_raw.duplicated(subset=GROUPBY_COLS+['results_uciid'], keep=False)])
logger.debug(f"Rows before dropping duplicates: {df_raw.shape[0]}")
# keep the first results_uciid as the smaller one is the correct one in the above cases 
# The goal is to keep these duplicated results_uciid, which have a unique rank for the race
df_raw = df_raw.drop_duplicates(GROUPBY_COLS+['results_uciid'], keep='first')
logger.debug(f"Rows after dropping duplicates: {df_raw.shape[0]}")


logger.debug(f"Number of races per race type:\n{df_raw.drop_duplicates(GROUPBY_COLS).racetype.value_counts()}")
logger.debug(f"Number of riders per season:\n{df_raw.groupby('seasonid').results_uciid.nunique()}")


race_results_list = []
race_types_extended = list(RACE_TYPES)
# use Omnium as proxy for Elimination if PROXY_ELIMINATION_WITH_OMNIUM=True and Elimination Race is in the RACE_TYPES list
if 'Elimination Race' in RACE_TYPES and PROXY_ELIMINATION_WITH_OMNIUM:
    race_types_extended.append('Omnium')
    
# iterate over the race_types_extended to filter the data to only the specified race types and clean the data
for race_type in race_types_extended:
    df_raw_race = df_raw[df_raw.racetype==race_type].copy()
    # clean the data of disqualified riders
    df_raw_race = preproc.remove_disqualified_riders(df_raw_race)
    # clean the data of races with only one rider
    df_raw_race = preproc.remove_one_rider_races(df_raw_race)
    
    # run some data quality checks
    logger.info('')
    logger.info(f'#### Run data quality checks on race type {race_type} ####')
    preproc.check_heat_stats(df_raw_race)
    logger.info('')
    
    race_results_list.append(df_raw_race)

df_race_results = pd.concat(race_results_list)

# duplicate names per uci_id
df_race_results['name'] = df_race_results.results_lastname + ' ' + df_race_results.results_firstname 
duplicate_names_per_uciid = df_race_results.groupby('results_uciid').name.agg(['nunique', 'unique']).reset_index()
if duplicate_names_per_uciid[duplicate_names_per_uciid['nunique']>1].empty:
    logger.warning(f"Duplicate names per UCIID:\n{duplicate_names_per_uciid[duplicate_names_per_uciid['nunique']>1]}")
    
# duplicate uci_id per name
duplicate_uciid_per_name = df_race_results.groupby('name').results_uciid.agg(['nunique', 'unique']).reset_index()
if not duplicate_uciid_per_name[duplicate_uciid_per_name['nunique']>1].empty:
    logger.warning(f"Duplicate names per UCIID:\n{duplicate_uciid_per_name[duplicate_uciid_per_name['nunique']>1]}")


# use Omnium as proxy for Elimination
if 'Elimination Race' in RACE_TYPES and PROXY_ELIMINATION_WITH_OMNIUM:
    df_race_results.racetype = df_race_results.racetype.replace('Omnium', 'Elimination Race')


# ## Race Stats

logger.debug('')
logger.debug('#### Race Stats ####')
unique_races = df_race_results[GROUPBY_COLS].drop_duplicates()
logger.debug("## All races (all data) ##")
logger.debug(f"Number of races overall: {unique_races.shape}")
logger.debug(f"Race distribution among race types:\n{unique_races.groupby('racetype').size().sort_index()}")

df_train, df_test, test_idx = preproc.train_test_split(df_race_results, EVALUATE_PERFORMANCE_TESTSEASON)

unique_races_train = df_train[GROUPBY_COLS].drop_duplicates()
logger.debug('')
logger.debug("## Races used for rating the riders (train data) ##")
logger.debug(f"Number of races overall: {unique_races_train.shape}")
logger.debug(f"Race distribution among race types:\n{unique_races_train.groupby('racetype').size().sort_index()}")

logger.debug('')
logger.debug("## Races used for evaluating the rider rating (test data) ##")
unique_races_test = df_test[GROUPBY_COLS].drop_duplicates()
logger.debug(f"Number of races overall: {unique_races_test.shape}")
logger.debug(f"Race distribution among race types:\n{unique_races_test.groupby('racetype').size().sort_index()}")

races_per_rider = df_race_results.groupby(['racetype', 'results_uciid']).size().to_frame(name='number_of_races_per_rider')
races_per_rider = races_per_rider.reset_index()
races_per_rider.sort_values(['racetype', 'number_of_races_per_rider'], ascending=[True, False]).groupby(['racetype']).head()

logger.debug('## Number of riders per race type (all data) ##')
n_riders_per_race = df_race_results.groupby(GROUPBY_COLS).results_uciid.nunique()
# determine the mode and median number of riders per race to have an estimate of randomly picking the correct winning rider
most_frequent_n_riders_per_race = n_riders_per_race.groupby(level=[2]).apply(lambda x: pd.Series.mode(x)[0]).rename('mode')
n_rider_stats = n_riders_per_race.groupby(level=[2]).agg(['mean', 'median', 'std', 'min', 'max', 'count'])
n_rider_stats = pd.concat([most_frequent_n_riders_per_race, n_rider_stats], axis=1)
logger.debug(f"\n{n_rider_stats}")
logger.info(f'Probabilities of correctly guessing the winning rider per race type:\n{1/most_frequent_n_riders_per_race}')


# check if number of unique UCIID and number of Ranks is the same per race
riders_unique_count = df_race_results.groupby(GROUPBY_COLS).agg({'results_uciid': ['nunique'], 'results_rank': ['count']})
riders_unique_count.columns = riders_unique_count.columns.droplevel(1)
if not riders_unique_count[riders_unique_count.results_rank!=riders_unique_count.results_uciid].empty:
    logger.warning(f"The following races of unequal number of unique ranks and UCIID:\n{riders_unique_count[riders_unique_count.results_rank!=riders_unique_count.results_uciid]}")


# Number of riders per race type displayed as histogram
number_of_ranks = df_race_results.groupby(GROUPBY_COLS).results_uciid.nunique()
if DEBUG_PLOTS: 
    for race_type in RACE_TYPES:
        plt.figure()
        number_of_ranks[:, :, race_type].hist(bins=20)
        plt.title(f"Number of riders per race for {race_type}")


if len(RACE_TYPES) > 1:
    for i, race_type in enumerate(RACE_TYPES):
        df_results_tmp = df_race_results[df_race_results.racetype==race_type]
        if i == 0:
            number_of_races_per_rider = df_results_tmp.results_uciid.value_counts().to_frame(name=f'{race_type}_races')
        else:
            number_of_races_per_rider = number_of_races_per_rider.merge(df_results_tmp.results_uciid.value_counts().to_frame(name=f'{race_type}_races'), how='outer', left_index=True, right_index=True)
    number_of_races_per_rider = number_of_races_per_rider.fillna(0)
    number_of_races_per_rider['races_overall'] = number_of_races_per_rider.sum(axis=1)
    number_of_races_per_rider = number_of_races_per_rider.sort_values('races_overall', ascending=False)
    logger.debug(f"Number of riders: {number_of_races_per_rider.shape[0]} in the race types: {RACE_TYPES}")
    logger.debug(f"Percentage of riders with more than 1 race: {100.0*(number_of_races_per_rider.races_overall>1).sum()/number_of_races_per_rider.shape[0]:.2f}%")
    logger.debug(f"Percentage of riders with more than 2 races: {100.0*(number_of_races_per_rider.races_overall>2).sum()/number_of_races_per_rider.shape[0]:.2f}%")
    logger.debug(f"Percentage of riders with more than 10 races: {100.0*(number_of_races_per_rider.races_overall>10).sum()/number_of_races_per_rider.shape[0]:.2f}%")
    if DEBUG_PLOTS:
        number_of_races_per_rider[number_of_races_per_rider.races_overall>10].drop('races_overall', axis=1).plot.bar(figsize=(30, 10), stacked=True, title=f'Number of races per rider on {RACE_TYPES[0]}/ {RACE_TYPES[1]} races')


# ## Train, predict and evalute performance
# The main methodology is using ELO scores to estimate rider's competency. The rider in a particular race with highest ELO score has the highest competency and thus highest likelihood of winning.

logger.info('')
logger.info('#### Calibrate ELO scores based on past race results ####')
# sort the dataframe by time to later iterate starting from first matches to last matches
df_race_results = df_race_results.sort_values(GROUPBY_COLS+['seasonid', 'timestamp'])
if EVALUATE_PERFORMANCE:
    df_train, df_test, test_idx = preproc.train_test_split(df_race_results, EVALUATE_PERFORMANCE_TESTSEASON)
    logger.debug(f"Size of train data: {df_train.shape}")
    logger.debug(f"Size of test data: {df_test.shape}")
else:
    df_train = df_race_results.copy()
    logger.debug(f"Size of train data: {df_train.shape}")
riders = df_train.results_uciid.drop_duplicates().tolist()

race_types_all = list(RACE_TYPES)
if len(RACE_TYPES) > 1 and 'all' not in RACE_TYPES:
    race_types_all = ['all'] + RACE_TYPES


# init
ts = trueskill.TrueSkill(draw_probability=0)
df_current_elos, df_elos_trajectory = elo_helper.init_elos(riders, df_train, ts)
# save the order the races are traversed in a dedicated dataframe to later sort them according to the race_order
df_race_order = pd.DataFrame(columns=['race_order'], index=df_elos_trajectory.index)


# train
df_current_elos, df_elos_trajectory = elo_helper.train_elos(df_train, df_current_elos, df_elos_trajectory, df_race_order, ts)


# predict
if EVALUATE_PERFORMANCE:
    df_riders_pred_list = []
    for race_type in race_types_all:
        if race_type == 'all':
            df_riders_pred_list.append(elo_helper.predict_elos(df_current_elos, df_race_results, df_train, df_test))
        else:
            df_riders_pred_list.append(elo_helper.predict_elos(df_current_elos, df_race_results, df_train, df_test[df_test.racetype==race_type]))


# evaluate
if EVALUATE_PERFORMANCE:
    rank_1_accuracy_list, rank_1_error_list = elo_helper.compute_rank_1_accuracy_and_error(df_riders_pred_list, race_types_all, EVALUATE_PERFORMANCE_N_BEST_RIDERS)


# ## Persisting output to Athena and DynamoDB

logger.info('')
logger.info('#### Write ELO scores per UCIID to S3/Athena and DynamoDB. ELO scores based on past race results ####')

df_current_elos.results_uciid = df_current_elos.results_uciid.astype(int)
df_current_elos.elo_mean = df_current_elos.elo_mean.round(4)
df_current_elos.elo_std = df_current_elos.elo_std.round(4)
for race_type in RACE_TYPES:
    df_result = df_current_elos.rename(columns={'results_uciid': 'uciid'}).copy()
    df_result['racetype'] = race_type.split(' ')[0]
    wr.s3.to_parquet(
        df=df_result,
        path=f's3://{target_bucket}/{target_table}/',
        dataset=True,
        database=target_database,
        table=target_table,
        mode='overwrite_partitions',
        partition_cols=['racetype'],
        boto3_session=session
    )
    io.put_elo_to_ddb(df_result, ddb_table, session)
    
print('Job Succeeded')




