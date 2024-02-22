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
from pyspark.ml.regression import LinearRegression, RandomForestRegressor, LinearRegressionModel
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.evaluation import RegressionEvaluator
from sklearn import linear_model
import numpy as np


LEARNING_RATE = 0.01
L1_PENALTY = 0.1
NUM_ITERS = 100
EPSILON = 0.000001

# Hyperparamters to tune for
POSSIBLE_REG_PARAMS = [0.001, 0.01, 0.1, 1, 10, 100]
POSSIBLE_ELASTIC_NET_PARAMS = [0.0, 0.25, 0.5, 0.75, 1.0]

def fit_linreg(train_df:DataFrame, save_path:str, target_col:str,
               elastic_net_param:float=0,
               reg_param:float=0,
               max_iter:float=100,
               drop_cols:list[str]=None, prefix:str="") -> None:
    """
    Fits dataframe to a linear regression model
    - Parameters
        - df               : PySpark Dataframe
        - save_path        : Path to save model in
        - target_col       : Column indicating the target feature
        - elastic_net_param: Parameters to determine ratio of L1 and L2 regularization
        - reg_param        : Regularization parameter (lambda)
        - max_iter         : Max iterations to run the gradient descent algorithm
        - drop_cols        :  Columns to drop if ever
        - prefix           : Used for access within other directories
    - Returns
        - None, but fits and saves a linear regression model from a given set of hyperparameters
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
    
    print("MAKING MODEL")
    lr = LinearRegression(featuresCol="features", labelCol=DOLLAR_VALUE, 
                          predictionCol=f"predicted_{DOLLAR_VALUE}",
                          maxIter=max_iter,
                          regParam=reg_param,
                          elasticNetParam=elastic_net_param,
                          )
    lr_model = lr.fit(data)

    # Now save the model
    print("SAVING MODEL")
    lr_model.write().overwrite().save(prefix+save_path)
    return

def evaluate_linreg(test_df:DataFrame, model_path:str,
                    save_path:str,
                    target_col:str,
                    metric_name:str="rmse",
                    prefix:str="") -> None:
    """
    Evaluates the linear regression model based on the model
    - Parameters
        - test_df    : The dataframe containing our test set
        - model_path : Path of the saved model
        - save_path  : Path to save results
        - target_col : Column to use as target variable
        - metric_name: Metric to use for results
        - prefix     : Used for access from different directories
    - Returns
        - None, but saves the evaluation metric of an instance of a model test with a set of hyperparameters
    """
    # Open our model
    lr_model = LinearRegressionModel.load(prefix+model_path)

    # Transform test data for evaluation
    input_cols = [col for col in test_df.columns if col != target_col]
    assembler = VectorAssembler(
        inputCols=input_cols,
        outputCol="features")
    data = assembler.transform(test_df)
    
    # First make the predictions
    predictions = lr_model.transform(data)
    
    # Now evaluate
    evaluator = RegressionEvaluator(labelCol=DOLLAR_VALUE, predictionCol=f"predicted_{DOLLAR_VALUE}", metricName=metric_name)
    score = evaluator.evaluate(predictions)

    # Now save our evaluations
    with open(save_path, "a") as fp:
        fp.write(str(score)+"\n")
    return

def get_tuned_params_linreg(rmse_path:str=LINREG_RMSE_SAVE_PATH, prefix:str="") -> tuple:
    """
    Gets the best parameters from the tuning results
    - Parameters
        - rmse_path: Where all the scores are located
        - prefix   : Prefix for access from other directories
    - Returns
        - A tuple containing the best fit hyperparameters
    """
    # First loop through the text file
    rmse_vals = []
    with open(prefix+rmse_path, "r") as fp:
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
    for reg_param in POSSIBLE_REG_PARAMS:
        for elastic_net in POSSIBLE_ELASTIC_NET_PARAMS:
            if curr_i == min_idx:
                return reg_param, elastic_net
            curr_i += 1
    return None