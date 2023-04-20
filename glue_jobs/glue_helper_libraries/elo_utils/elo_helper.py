import pandas as pd
import time
from elo_utils import GROUPBY_COLS, logger

def init_elos(riders: list, df_train: pd.DataFrame, ts):
    """
    Initialize elo scores with default rating from trueskill.
    
    Arguments:
        riders (list) - list of unique rider UCIIDS
        df_train (pd.DataFrame) - training data used for computing the elo score per rider
        ts (trueskill.TrueSkill) - TrueSkill object
    """
    # creating a dictionary with one default value Rating object for each account_id
    current_elos = dict()
    for r in riders:
        current_elos[r] = ts.create_rating()
    df_current_elos = pd.Series(current_elos)
    df_elos_trajectory = pd.concat([df_train[GROUPBY_COLS].drop_duplicates(), 
                                  pd.DataFrame(columns=riders)], axis=1).set_index(GROUPBY_COLS)
    
    return df_current_elos, df_elos_trajectory


def train_elos(df_train: pd.DataFrame, current_elos: pd.DataFrame, df_elos_trajectory: pd.DataFrame, df_race_order: pd.DataFrame, ts):
    """
    Calibrate the elo scores by updating them race by race. 
    
    Arguments:
        df_train (pd.DataFrame) - training data used for computing the elo score per rider
        current_elos (pd.DataFrame) - the elo score which is iteratively updated per rider
        df_elos_trajectory (pd.DataFrame) - DataFrame containing the elo score per rider and per race with dimensions: (#races, #riders)
        df_race_order (pd.DataFrame) - DataFrame containing the order of races used for calibrating the elo scores
        ts (trueskill.TrueSkill) - TrueSkill object
    """
    # iterate thorugh matches to update scores
    start_time = time.time()
    for race_id, (grp_idx, grp) in enumerate(df_train.groupby(GROUPBY_COLS)):
        # get the before match elo score and rank of the match
        elo_score_before_match = current_elos.loc[grp.results_uciid.values].values.tolist()
        elo_score_before_match = [(r, ) for r in elo_score_before_match]
        ranks_match = list(grp.results_rank.values - 1)
        elo_score_after_match = ts.rate(elo_score_before_match, ranks=ranks_match)
        # update the elo scores with the updated scores after the match
        current_elos.loc[grp.results_uciid.values] = list(zip(*elo_score_after_match))[0]
        df_elos_trajectory.loc[grp_idx] = current_elos
        df_race_order.loc[grp_idx] = race_id
    df_elos_trajectory = df_elos_trajectory.merge(df_race_order, left_index=True, right_index=True, how='left')
    df_elos_trajectory = df_elos_trajectory.sort_values('race_order')
    
    # expand elo mean and std from rider elo
    df_current_elos = current_elos.to_frame(name='elo_rating')
    df_current_elos['elo_mean'] = df_current_elos.elo_rating.apply(lambda x: x.mu)
    df_current_elos['elo_std'] = df_current_elos.elo_rating.apply(lambda x: x.sigma)
    df_current_elos = df_current_elos.drop('elo_rating', axis=1)
    df_current_elos = df_current_elos.rename_axis('results_uciid').reset_index()
    
    end_time = time.time()
    logger.debug(f"Time taken to calibrate the elo scores with training data: {end_time-start_time:.2f}")
    
    return df_current_elos, df_elos_trajectory

def predict_elos(df_current_elos: pd.DataFrame, df_train_test: pd.DataFrame, df_train: pd.DataFrame, df_test: pd.DataFrame):
    """
    Predict the winning rider for test data based on the current elos. The rider per race having the highest elo score gets predicted as the winning rider
    
    Arguments:
        df_current_elos (pd.DataFrame) -  the most up to date elo score per rider
        df_train (pd.DataFrame) - training data used for computing the elo score per rider
        df_train_test (pd.DataFrame) - train + test data i.e. all race results
        df_test (pd.DataFrame) - test data to evalute performance of calibrated elo scores on
    """
    df_pred_per_racetype = []
    # number of races per rider within training data
    races_per_rider_train = df_train.results_uciid.value_counts().rename_axis('results_uciid').to_frame(name='races_per_rider').reset_index()
    # iterate through test data to map the rider elo scores to it
    for grp_idx, grp in df_test.groupby(GROUPBY_COLS):
        df_riders_pred = grp.merge(df_current_elos, on='results_uciid', how='left')
        # merge the number of races per rider
        df_riders_pred = df_riders_pred.merge(races_per_rider_train, on='results_uciid', how='left')
        # predict the rank based on the sorting for each rider on the elo score
        df_riders_pred = df_riders_pred.sort_values(GROUPBY_COLS+['elo_mean'], ascending=False)
        df_riders_pred['pred_rank'] = 1
        df_riders_pred.pred_rank = df_riders_pred.groupby(GROUPBY_COLS).pred_rank.cumsum()
        # merge the true rank to it
        df_riders_pred = df_riders_pred.merge(df_train_test[GROUPBY_COLS+['results_uciid', 'results_rank']].rename(columns={'results_rank': 'actual_rank_orig'}), 
                                              on=GROUPBY_COLS+['results_uciid'], how='left')
        # the true rank could be e.g. 3, 4 subtract the min rank per race from these actual ranks to get ranks starting from 1
        df_riders_pred['actual_rank'] = df_riders_pred.actual_rank_orig - df_riders_pred.groupby(GROUPBY_COLS).actual_rank_orig.transform('min') + 1
        df_riders_pred['pred_err'] = (df_riders_pred.actual_rank - df_riders_pred.pred_rank).abs()
        df_pred_per_racetype.append(df_riders_pred)
    
    return pd.concat(df_pred_per_racetype)

def compute_rank_1_accuracy_and_error(df_pred_per_racetype: list, race_types: list, n_best: int):
    """
    Evalute the prediction vs. the actual rank 1 by computing the accuracy, error (rank distance to the 1st rider) and number of null values (unknown elo score) for the actual rank 1 riders
    
    Arguments:
        df_pred_per_racetype (list[pd.DataFrame]) - list of DataFrames containing the predicted and actual ranks per race
        race_types (list) - list of race types containing the race types in the same order as df_pred_per_racetype
        n_bist (int) - number of best riders to consider as favourite rider
    """
    rank_1_accuracy_list, rank_1_error_list, rank_1_null_list = [], [], []
    for i, (race_type, df) in enumerate(zip(race_types, df_pred_per_racetype)):
        rank_1_accuracy_list.append((df[df.pred_rank<=n_best].groupby(GROUPBY_COLS).actual_rank.min() == 1).sum()/df[df.actual_rank==1].shape[0])
        rank_1_error_list.append((df[df.actual_rank==1].pred_rank - 1).abs().sum()/df[df.actual_rank==1].shape[0])
        rank_1_null_list.append(df[df.actual_rank==1].elo_mean.isnull().sum()/df[df.actual_rank==1].shape[0])
        logger.info(f"""rank_1_accuracy_{race_types[i]}: {rank_1_accuracy_list[i]}, rank_1_error_{race_types[i]}: {rank_1_error_list[i]}, rank_1_null_{race_types[i]}: {rank_1_null_list[i]}""")
    
    return rank_1_accuracy_list, rank_1_error_list