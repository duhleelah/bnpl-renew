from .constants import *
from .read import *
from .load import *
from .misc_changes import *
import re
import pandas as pd
import random
import json
import  pyspark.sql.functions as F
from pyspark.sql.types import *
import numpy as np
from pyspark.ml.regression import LinearRegression, RandomForestRegressor, LinearRegressionModel, RandomForestRegressionModel
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.evaluation import RegressionEvaluator
from sklearn import linear_model
import numpy as np
import math
from statsmodels.tsa.arima.model import ARIMA
from statsmodels.tsa.seasonal import seasonal_decompose
from sklearn.metrics import mean_squared_error
import warnings
from statsmodels.tools.sm_exceptions import ConvergenceWarning

# Ignore convergence warning with Time Series model
warnings.simplefilter('ignore', ConvergenceWarning)

P_VALS = [1, 3, 5, 7, 10]
Q_VALS = [1, 3, 5, 7, 10]
TS_COMBINATIONS = len(P_VALS) * len(Q_VALS)
TS_INSERT_RMSE = 100000

def  fit_ts(train_df:DataFrame, test_df:DataFrame, target_col:str,
            p:int =1, q:int=1) -> float:
    """
    Fits dataframe to a time series model
    - Parameters
        - train_df  : PySpark Train Dataframe
        - test_df   : Pyspark Test Dataframe
        - save_path : Path to save model in
        - target_col: Column indicating the target feature
        - p         : Hyperparameter denoting how many of the previous values to include in the model
        - q         : Hyperparameter denoting how many of the previous errors to include in the model
    - Returns
        - Float value denoting the Root Mean Squared Error (RMSE)
    """

    # Convert the datframe into Pandas dataframes
    # train_df = train_df.toPandas()
    if ORDER_DATETIME in train_df.columns:
        train_df.set_index(ORDER_DATETIME, inplace=True)
    train_df_endog = train_df[target_col]
    train_df_exog = train_df.drop([target_col], axis=1)
    if ORDER_DATETIME in test_df.columns:
        test_df.set_index(ORDER_DATETIME, inplace=True)
    test_df_endog = test_df[target_col]
    test_df_exog = test_df.drop([target_col], axis=1) 

    # Now model the ARIMA
    model = ARIMA(endog=train_df_endog, order=(p, 1, q),
                        exog=train_df_exog)
    model_fit = model.fit()
    forecast_steps = len(test_df)
    predicted = model_fit.forecast(steps=forecast_steps,
                                        exog=test_df_exog)
    rmse = mean_squared_error(test_df_endog, predicted, squared=False)
    print(f"RMSE on test data: {rmse:.3f}")
    return rmse

def ts_initialize_param_txt_file(save_path:str, prefix:str="") -> None:
    """
    Initializes the text file to store all the possible parameter tuning
    - Parameters
        - save_path: Path to store all the parameter tuning
        - prefix   : Prefix for access from other directories
    - Returns
        - None, but saves the text file used to store the results 
        of hyperparamter tuning for a Time Series
    """
    initialize_str = "\n".join([str(float(val)) for val in TS_COMBINATIONS*[TS_INSERT_RMSE]])
    with open(prefix+save_path, "w") as fp:
        fp.write(initialize_str)
    return

def hyperparameters_ts(train_df:DataFrame, test_df:DataFrame,
                              save_path:str,
                              target_col:str=DOLLAR_VALUE,
                              prefix:str="") -> None:
    """
    Does one round of the hyperparameter tuning for each of the possible parameters
    - Parameters
        - train_df  : Training Dataframe
        - test_df   : Testing Dataframe
        - save_path : Used to store the current values in this iteration
        - target_col: Target variable of the dataset 
        - prefix    : Prefix used for access in other directories
    - Returns
        - None, but writes down the current RMSE results of a single iteration 
        of each possible hyperparameter combination
    """
    # Tuning
    rmse_scores = []
    for p in P_VALS:
        for q in Q_VALS:
            curr_rmse = fit_ts(train_df, test_df, target_col, 
                                      p, q)
            rmse_scores.append(curr_rmse)

    # Append each of the values in the given text file
    curr_rmses = []
    with open(prefix+save_path, "r") as fp:
        # First open the current values
        curr_rmses = [float(val) for val in fp.read().split("\n")]
    for i in range(len(curr_rmses)):
        curr_rmses[i] += rmse_scores[i]
    # Now write it into the file
    write_str = "\n".join([str(val) for val in curr_rmses])
    with open(prefix+save_path, "w") as fp:
        fp.write(write_str)
    return

def get_tuned_params_ts(params_path:str, prefix:str="") -> tuple:
    """
    Selects the best parameters based on the lowest RMSE
    - Parameters
        - params_path: Text file containing all the RMSE calculations
        - prefix     : Prefix used for access from other directories
    - Returns
        - Tuple containing all the needed parameters
    """

    min_rmse = float("inf")
    min_index = -1

    with open(prefix+params_path, "r") as fp:
        curr_vals = [float(val) for val in fp.read().split("\n")]
        for i in range(len(curr_vals)):
            if curr_vals[i]/NUM_VALIDATIONS < min_rmse:
                min_rmse = curr_vals[i]/NUM_VALIDATIONS
                min_index = i
    
    # Now return the necessary values
    count = 0
    for p in P_VALS:
        for q in Q_VALS:
            if count == min_index:
                return p, q
            count += 1
    return None