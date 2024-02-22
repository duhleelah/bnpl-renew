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
from keras.models import Sequential
from keras.layers import Dense
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import mean_squared_error
from itertools import combinations

# Hyperparameters to tune
NEURAL_NET_COMBINATIONS = 9
BATCH_SIZES = [20, 100, 200]
EPOCHS = [1, 3, 5]
INSERT_RMSE = 100000

def fit_neuralnet(train_df:DataFrame, test_df:DataFrame,
                   target_col:str,
                   batch_sizes:int=20,
                   epochs:int=5) -> float:
    """
    Fits a neural network regression model with 1 hidden layer
    - Parameters
        - train_df   : Pandas Dataframe for training
        - test_df    : Pandas Dataframe for testing
        - target_col : Column indicating the target feature
        - batch_sizes: Batch size to use for neural network
        - epochs     : Number of epochs to use for neural network
    - Returns
        - Root Mean Squared Error (RMSE) resulting from mode
    """
    # Convert the datframe into Pandas dataframes
    train_df_y = train_df[target_col]
    train_df_x = train_df.drop([target_col], axis=1)
    test_df_y = test_df[target_col]
    test_df_x = test_df.drop([target_col], axis=1)

    # Have to scale everything first
    x_scaler = StandardScaler()
    # Need to get indices of train set
    train_idx_x = train_df.shape[0]
    x_dfs = pd.concat([train_df_x, test_df_x])
    x_scaler_fit = x_scaler.fit(x_dfs)
    x_dfs = x_scaler_fit.transform(x_dfs)
    train_df_x = x_dfs[:train_idx_x, :]
    test_df_x = x_dfs[train_idx_x:, :]
    
    y_scaler = StandardScaler()
    train_idx_y = train_df.shape[0]
    y_dfs = pd.concat([train_df_y, test_df_y]).to_numpy().reshape(-1, 1)
    y_scaler_fit = y_scaler.fit(y_dfs)
    y_dfs = y_scaler_fit.transform(y_dfs)
    train_df_y = y_dfs[:train_idx_y, :]
    test_df_y = y_dfs[train_idx_y:, :]

    # create ANN model
    model = Sequential()

    # Defining the Input layer and FIRST hidden layer, both are same!
    model.add(Dense(units=5, input_dim=len(train_df.columns)-1, kernel_initializer='normal', activation='relu'))
    model.add(Dense(1, kernel_initializer='normal'))

    # Compiling the model
    model.compile(loss='mean_squared_error', optimizer='adam')

    # Fitting the ANN to the Training set
    model.fit(train_df_x, train_df_y ,batch_size = batch_sizes, epochs = epochs, verbose=0)
    predictions = model.predict(test_df_x)
    predictions = y_scaler_fit.inverse_transform(predictions)
    test_df_y = y_scaler_fit.inverse_transform(test_df_y)
    rmse = mean_squared_error(test_df_y, predictions, squared=False)
    print(f"RMSE on test data: {rmse:.3f}")
    return rmse

def neuralnet_initialize_param_txt_file(save_path:str, prefix:str="") -> None:
    """
    Initializes the text file to store all the possible parameter tuning
    - Parameters
        - save_path: Path to store all the parameter tuning
        - prefix   :Prefix for access from other directories
    - Returns
        - None, but saves the file storing the evaluation values needed to determine best hyperparameters
    """
    
    initialize_str = "\n".join([str(float(val)) for val in NEURAL_NET_COMBINATIONS*[INSERT_RMSE]])
    with open(prefix+save_path, "w") as fp:
        fp.write(initialize_str)
    return

def hyperparameters_neuralnet(train_df:DataFrame, test_df:DataFrame,
                              save_path:str,
                              target_col:str=DOLLAR_VALUE,
                              prefix:str="") -> None:
    """
    Does one round of the hyperparameter tuning for each of the possible parameters
    :param: train_df - Training Dataframe
    :param: test_df - Testing Dataframe
    :param: save_path - Used to store the current values in this iteration
    :param: prefix - Prefix used for access in other directories
    """
    # Tuning
    rmse_scores = []
    for size in BATCH_SIZES:
        for epoch in EPOCHS:
            curr_rmse = fit_neuralnet(train_df, test_df, target_col, 
                                      size, epoch)
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

def get_tuned_params_neuralnet(params_path:str, prefix:str="") -> tuple:
    """
    Selects the best parameters based on the lowest RMSE
    - Parameters
        - params_path - Text file containing all the RMSE calculations
        - prefix - Prefix used for access from other directories
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
    for size in BATCH_SIZES:
        for ep in EPOCHS:
            if count == min_index:
                return size, ep
            count += 1
    return None