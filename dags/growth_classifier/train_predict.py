import pandas as pd
import numpy as np
from datetime import timedelta
import datetime as dt

## modeling
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import RandomizedSearchCV

sm_data_lake_dir = '/Users/alexanderhubbard/stock-market/data'
TRAINING_DIR = sm_data_lake_dir+'/training-data/growth-stock-selector'
PREDICTIONS_DIR = sm_data_lake_dir+'/predictions/growth-stock-selector'

import common.scripts.utils as utils

############################################
##### Set up grid search and freatures #####
############################################

# Number of trees in random forest
n_estimators = [int(x) for x in np.linspace(start = 200, stop = 300, num = 10)] #2000
# Number of features to consider at every split
max_features = ['auto', 'sqrt']
# Maximum number of levels in tree
max_depth = [int(x) for x in np.linspace(10, 110, num = 11)]
max_depth.append(None)
# Minimum number of samples required to split a node
min_samples_split = [2, 5, 10]
# Minimum number of samples required at each leaf node
min_samples_leaf = [1, 2, 4]
# Method of selecting samples for training each tree
bootstrap = [True, False]
# Create the random grid
RANDOM_GRID = {'n_estimators': n_estimators,
               'max_features': max_features,
               'max_depth': max_depth,
               'min_samples_split': min_samples_split,
               'min_samples_leaf': min_samples_leaf,
               'bootstrap': bootstrap}
Y_CLASS = 'growth'
TRAINING_FEATURES = [
    'consolidated_net_income',
    'net_investing_cash_flow',
    'total_assets',
    'net_income_after_extraordinaries_slope',
    'avg_volume',
    'consolidated_net_income_slope',
    'funds_from_operations',
    'common_stock_parcarry_value',
    'net_income_available_to_common_slope',
    'net_financing_cash_flow',
    'other_liabilities',
    'pretax_income',
    'st_debt_and_current_portion_lt_debt',
    'total_shareholders_equity_total_assets',
    'close',
    'eps_diluted_slope',
    'common_equity_total_assets',
    'change_in_capital_stock',
    'long_term_debt',
    'total_equity',
    'capital_expenditures',
    'changes_in_working_capital',
    'diluted_shares_outstanding_slope',
    'retained_earnings',
    'eps_basic',
    'diluted_shares_outstanding',
    'capital_expenditures_fixed_assets',
    'liabilities_and_shareholders_equity',
    'depreciation_and_amortization_expense',
    'eps_basic_slope',
    'net_income_slope',
    'basic_shares_outstanding_slope',
    'short_term_debt',
    'net_income_available_to_common',
    'other_assets',
    'basic_shares_outstanding',
    'other_liabilities_excl_deferred_income',
    'salesrevenue',
    'total_liabilities',
    'issuancereduction_of_debt_net',
    'net_change_in_cash',
    'common_equity_total',
    'pretax_income_slope',
    'avg_price',
    'net_income',
    'total_shareholders_equity',
    'eps_diluted',
    'net_operating_cash_flow',
    'intangible_assets',
    'net_income_after_extraordinaries',
    'free_cash_flow'
]

###################
##### Methods #####
###################
def split_test_train(date: dt.datetime, df: pd.DataFrame) -> pd.DataFrame:
    """
    Split the full training set into
    the training set, will no missing data,
    and the test set, for the given day.
    """
    ## subset DF, no null features or class
    df_cp = (
        df.loc[df[TRAINING_FEATURES+[Y_CLASS]].isnull().sum(1) == 0]
        .copy()
    )
    train_data = df_cp.loc[df_cp['date'] < date]
    test_data = df_cp.loc[df_cp['date'] == date]
    
    return train_data, test_data

def load_training(date: dt.date):
    """
    Load training data for model.
    
    TO DOs
    - Add logic to prevent small training set.
    """
    ## load and type
    usecols = (
        [Y_CLASS] + 
        TRAINING_FEATURES + 
        ['ticker', 'date', 'future_date', 
         'future_close', 'future_avg_close']
    )
    fp = TRAINING_DIR + '/{}.csv'.format(date)
    full_training_df = pd.read_csv(fp, usecols=usecols)
    for col in ['future_date', 'date']:
        full_training_df[col] = (
            pd.to_datetime(full_training_df[col])
            .apply(lambda r: r.date())
        )
    
    ## split
    train_df, test_df = split_test_train(date, full_training_df)
    return train_df, test_df

def train_and_predict(date: dt.date, threshold=.7) -> pd.DataFrame:
    """
    Perform hyperparametere tuning, train model with
    best params, make predictions on data for given
    trading date.
    
    Inputs:
        - Test data
        - Training data
        - Probability threhold, default .7
    """
    ## for type 
    date = pd.to_datetime(date).date()
    ## load training data
    train_df, test_df = load_training(date)
    ## split by features
    X = train_df[TRAINING_FEATURES]
    y = train_df[Y_CLASS].astype(int)
    X_test = test_df[TRAINING_FEATURES]
    y_test = test_df[Y_CLASS].astype(int)
    ## auto tune
    rf_tuner = RandomForestClassifier()
    RSCV = RandomizedSearchCV(rf_tuner, RANDOM_GRID, cv=3, verbose=1, n_jobs=-1)
    RSCV_results = RSCV.fit(X, y)
    ## Instantiate with best params
    rf_clf = RSCV_results.best_estimator_
    # fit a model
    rf_clf.fit(X, y)
    # predict probabilities
    probs = rf_clf.predict_proba(X_test)
    ## set other variables
    predict_df = pd.DataFrame(probs, index=y_test.index, columns=['p_not_growth', 'p_growth'])
    predict_df['predict'] = (predict_df['p_growth'] >= threshold).astype(int)
    predict_df['actual'] = y_test
    predict_df['predict_date'] = date
    predict_df['threshold'] = threshold
    predict_df['avg_price'] = test_df['avg_price']
    predict_df['avg_volume'] = test_df['avg_volume']
    predict_df['ticker'] = test_df['ticker']
    predict_df['future_close'] = test_df['future_close']
    predict_df['future_avg_close'] = test_df['future_avg_close']
    predict_df['price_change_percent'] = (
        test_df['future_close']
        / test_df['avg_price']
    )
    ## save
    date_str = str(date)[:10]
    predict_df.to_csv(PREDICTIONS_DIR + '/{}.csv'.format(date_str), index=False)
    return True
