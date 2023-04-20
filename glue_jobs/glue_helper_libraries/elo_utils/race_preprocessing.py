import pandas as pd
import time
from elo_utils import GROUPBY_COLS, logger

def remove_disqualified_riders(df: pd.DataFrame):
    """
    Remove disqualified riders. 
    
    Riders with Rank which is not > 0 are disqualified. 
    
    Arguments:
        df (pd.DataFrame) - DataFrame containing the race result rankings
    """
    df = df[(df.results_rank>0) & (~df.results_status.isin(['DNF', 'DNS', 'DSQ', 'REL']))]
    return df

def remove_one_rider_races(df: pd.DataFrame):
    """
    Filter out heats which have only one rider, as this is not a eligible race having a winner or loser.
    
    Arguments:
        df (pd.DataFrame) - DataFrame containing the race result rankings
    """
    df['ranks_per_race'] = df.groupby(GROUPBY_COLS).results_rank.transform('count')
    # filter on heats with more than one rider
    df = df[df.ranks_per_race > 1]
    return df


def check_heat_stats(df: pd.DataFrame): 
    """
    Checks performed per heat and rank.
    Check 1: check if max rank, min rank and therefore rank count match up per heat
    Check 2: check for duplicated ranks per heat and rank
    Check 3: check if difference between subsequent ranks is not 1
    
    Arguments:
        df (pd.DataFrame) - DataFrame containing the race result rankings
    """
    df['min_rank_per_race'] = df.groupby(GROUPBY_COLS).results_rank.transform('min')
    df['max_rank_per_race'] = df.groupby(GROUPBY_COLS).results_rank.transform('max')
    
    # check if max rank, min rank and therefore rank count match up
    check1 = df[df.max_rank_per_race - df.min_rank_per_race + 1 != df.ranks_per_race]
    if not check1.empty:
        if check1.shape[0] <= 10:
            display(check1)
        number_of_cases = check1.drop_duplicates(subset=GROUPBY_COLS)
        logger.warning(f"Failed check 1: Number of ranks is unequal to difference between max and min rank in {number_of_cases.shape[0]} number of cases.")
    
    # Check for duplicated ranks per heat and rank
    check_duplicated_ranks = df[df.duplicated(subset=GROUPBY_COLS+['results_rank'], keep=False)]
    if not check_duplicated_ranks.empty:
        if check_duplicated_ranks.shape[0] <= 10:
            display(check_duplicated_ranks)
        number_of_duplicates = check_duplicated_ranks.drop_duplicates(subset=GROUPBY_COLS+['results_rank'])
        logger.warning(f"Failed duplicated ranks. {number_of_duplicates.shape[0]} Duplicates in {GROUPBY_COLS+['results_rank']}.")
    
    df = df.sort_values(GROUPBY_COLS+['results_rank'])
    df['rank_diff'] = df.groupby(GROUPBY_COLS).results_rank.apply(lambda x: x) - df.groupby(GROUPBY_COLS).results_rank.shift(1)
    check3 = df[~((df.rank_diff==1) | (df.rank_diff.isnull()))]
    if not check3.empty:
        if check3.shape[0] <= 10:
            display(check3)
        logger.warning(f"Failed check 3: difference between subsequent ranks is not always 1 or null. In {check3.shape[0]} cases this is not fulfilled.")
        
    return 0

def train_test_split(df: pd.DataFrame, test_season: list):
    """
    Split the historic UCI data into train and test data. Train data is everything before the season of 2020 and test data is season 2020 and 2021.
    
    Arguments:
        df (pd.DataFrame) - DataFrame containing the race rankings of past races
        test_season (list) - list of seasonid to use for test data
    """
    test_idx = (df.seasonid.isin(test_season))
    df_train = df[~test_idx].copy()
    df_test = df[test_idx].copy()
    n_overall_races = df[GROUPBY_COLS].drop_duplicates().shape[0]
    n_train_races = df_train[GROUPBY_COLS].drop_duplicates().shape[0]
    n_test_races = df_test[GROUPBY_COLS].drop_duplicates().shape[0]
    logger.info(f"Number of Heats/races in test data: {n_test_races}.\nPercentage split of races in train/test: {100.0*n_train_races/n_overall_races:.2f}%/{100.0*n_test_races/n_overall_races:.2f}%")
    return df_train, df_test, test_idx