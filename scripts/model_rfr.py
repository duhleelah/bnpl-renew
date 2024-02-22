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

# Hyperparameters to tune
NUM_TREES = [5, 10, 20]
MAX_DEPTH = [3, 5, 10]

def fit_rfr(train_df:DataFrame, save_path:str, count_path:str, 
               target_col:str,
               prediction_col_name:str,
               max_depth:int=5,
               num_trees:int=20,
               drop_cols:list[str]=None, prefix:str="") -> None:
    """
    Fits dataframe to a random forest regression model
    - Parameters
        - train_df           : PySpark Dataframe of training data
        - save_path          : Path to save model in
        - count_path         : Path to save the counts for weighted average
        - target_col         : Column indicating the target feature
        - prediction_col_name: Prediction column used for batch evaluation
        - max_depth          : Max depth a decision tree in a Random Forest will go
        - num_trees          : Number of trees in Random Forest
        - drop_cols          : Columns to drop if ever
        - prefix             : Used for access within other directories
    - Returns
        - None, but saves the given RFR model and the number of instances it worked with
    """
    # First drop columns if ever
    if drop_cols:
        train_df = drop_columns(train_df, drop_cols)

    # Now fit the regression model
    input_cols = [col for col in train_df.columns if col != target_col]
    assembler = VectorAssembler(
        inputCols=input_cols,
        outputCol="features")
    
    data = assembler.transform(train_df)
    
    rfr = RandomForestRegressor(featuresCol="features", labelCol=DOLLAR_VALUE, 
                          predictionCol=prediction_col_name,
                          maxDepth=max_depth,
                          numTrees=num_trees)
    rfr_model = rfr.fit(data)

    # Now save the model
    rfr_model.write().overwrite().save(prefix+save_path)
    # Save the count of the model
    with open(prefix+count_path, "w") as fp:
        fp.write(str(train_df.count()))
    return

def predict_rfr(test_df:DataFrame, model_path:str,
                    save_path:str,
                    target_col:str,
                    prediction_col:str,
                    prefix:str="") -> None:
    """
    Evaluates the random forest regression models, difference is
    we calculate a weighted average of all the possible models
    - Parameters
        - test_df       : The dataframe containing our test set
        - model_path    : Path of the saved model
        - save_path     : Path to save results
        - target_col    : Column containing the target variable
        - prediction_col: Column used to predict
        - prefix        : Used for access from different directories
    - Returns
        - None, but saves the predictions of a given RFR model
    """
    # Open our model
    rfr_model = RandomForestRegressionModel.load(prefix+model_path)

    # Transform test data for evaluation
    input_cols = [col for col in test_df.columns if col != target_col]
    assembler = VectorAssembler(
        inputCols=input_cols,
        outputCol="features")
    data = assembler.transform(test_df)
    
    # Make the predictions
    predictions = rfr_model.transform(data)
    predidcted_vals = [str(row[prediction_col]) for row in predictions.select(F.col(prediction_col)).collect()]

    # Save the predicted values
    with open(save_path, "w") as fp:
        fp.write("\n".join(predidcted_vals))
    return

def calculate_rmse(test_df:DataFrame,
                   target_col:str,
                   save_path:str,
                   predicted_paths:list[str],
                   count_paths:list[str],
                   prefix:str="") -> None:
    """
    Calculates the RMSE for the Random Forest Regression Models
    - Parameters
        - test_df        : Test dataframe to get true values
        - target_col     : Target column for the dataframe
        - save_path      : Path to save the calculated RMSE's for the given model
        - predicted_paths: The paths of the text files contaning the predictions
        - count_paths    : The paths of the text files containing the number of instances each model trained on
        - prefix         : Prefix for access in different directories
    - Returns
        - None, but saves results into necessary files
    """
    # First read in the counts and predictions of the dataframes
    total_counts = []
    for i in range(len(count_paths)):
        curr_counts = 0
        with open(prefix+count_paths[i], "r") as fp:
            curr_counts = int(fp.read())
        total_counts.append(curr_counts)


    agg_predictions = []
    for i in range(len(predicted_paths)):
        curr_predictions = []
        with open(prefix+predicted_paths[i], "r") as fp:
            curr_predictions = [float(val) for val in fp.read().split("\n")]

        if i == 0:
            agg_predictions = [(total_counts[i]/sum(total_counts))*val for val in curr_predictions]
        else:
            agg_predictions = [agg_predictions[j] + (total_counts[i]/sum(total_counts))*curr_predictions[j] \
                               for j in range(len(curr_predictions))]
            
    # Get true values
    true_values = [row[target_col] for row in test_df.select(F.col(target_col)).collect()]
    
    # Now calculate the RSME
    total = sum(total_counts)
    error_2 = [(agg_predictions[i] - true_values[i])**2/total for i in range(len(true_values))]
    rmse = math.sqrt(sum(error_2))
    print(rmse)
    print(f"RMSE on test data: {rmse:.3f}")
    # Save the results
    # print(prefix+save_path)
    with open(prefix+save_path, "a") as fp:
        fp.write(str(rmse)+"\n")
    return

def get_tuned_params_rfr(rmse_path:str=RFR_RMSE_SAVE_PATH, prefix:str="") -> tuple:
    """
    Gets the best parameters from the tuning results
    - Parameters
        - rmse_path: Where all the scores are located
        - prefix   : Prefix for access from other directories
    - Returns
        - Tuple containing hyperparameters from Random Forest Regression
    """
    # First loop through the text file
    rmse_vals = []
    with open(rmse_path, "r") as fp:
        rmse_vals = [float(val) for val in fp.read().split("\n")[:-1]]
    
    # Now loop to average all the values
    curr_vals_set = []
    avg_rmse_vals = []
    for i in range(len(rmse_vals)):
        if (i+1)%TRAIN_BATCH == 0:
            avg_rmse_vals.append(sum(curr_vals_set)/TRAIN_BATCH)
            curr_vals_set = []
        else:
            curr_vals_set.append(rmse_vals[i])
    
    # Afterwards, we loop through the averaged vals and find the max index
    min_rmse = float("inf")
    min_idx = -1

    for i in range(len(avg_rmse_vals)):
        curr_rmse = avg_rmse_vals[i]
        if curr_rmse < min_rmse:
            min_rmse = curr_rmse
            min_idx = i
    
    # Now loop through the possible parameters to find the 
    # optimal params
    curr_i = 0
    for depth in MAX_DEPTH:
        for trees in NUM_TREES:
            if curr_i == min_idx:
                return depth, trees
            curr_i += 1
    return None