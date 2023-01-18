import pandas as pd
import polars as pl

def predict_fraud(df, method='rule'):
    pass

def set_rule(key, val, threshold):
    pass

def predict_by_rule(df):
    df['rule_score'] = (df['amount'] > 10000000).astype(int) +\
                       (df['freq'] > 5).astype(int) +\
                       (df['count_location'] > 1).astype(int)+\
                       (df['hour'] <= 5 and df['hour'] >= 0).astype(int)

    return df

def predict_by_model():
    pass

def predict_fraud(df, method='all'):
    """
    method: [all,rule,model]
    """
    pass