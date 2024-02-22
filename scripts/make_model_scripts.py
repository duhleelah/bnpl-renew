from .constants import *
from .read import *
from .load import *
from .misc_changes import *
from .model_rfr import *
from .model_linreg import *
from .model_ts import *
from .model_neuralnet import *
from .make_run_file import *
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

MAKE_MODEL_SCRIPTS = "make_model_scripts"
REMOVE_PY_VAL = -3

# -------------------------------------- SHELL COMMANDS --------------------------------------
PYTHON_CONSOLE_FORMAT = "python3 -m scripts.{}"
LINREG_CONSOLE_FORMAT = "python3 -m scripts.hypertune_linreg.{}"
RFR_CONSOLE_FORMAT = "python3 -m scripts.hypertune_rfr.{}"
TS_CONSOLE_FORMAT = "python3 -m scripts.hypertune_ts.{}"
NEURALNET_CONSOLE_FORMAT = "python3 -m scripts.hypertune_neuralnet.{}"

# -------------------------------------- LINREG MODELS --------------------------------------
LINREG_MODEL_SAVE_PATH_FORMAT = "./data/curated/hypertune_linreg_models/hypertune_linreg_models_{}/"

LINREG_NAME_MODEL_FORMAT = "linreg_model_{}.py"
LINREG_FILE_MODEL_FORMAT = """from ..constants import *
from ..read import *
from ..model_linreg import *

if __name__ == "__main__":
    spark = create_spark()
    train_df = read_train_data(spark)
    fit_linreg(train_df, {}, {}, {}, {})
"""

LINREG_NAME_EVAL_FORMAT = "linreg_evaluate_{}_{}.py"
LINREG_FILE_EVAL_FORMAT = """from ..constants import *
from ..read import *
from ..model_linreg import *

if __name__ == "__main__":
    spark = create_spark()
    test_df = read_test_data(spark)
    test_df = test_df.sample(withReplacement=True, fraction=1.0)
    evaluate_linreg(test_df, {}, {}, {})
"""

# -------------------------------------- RFR MODELS --------------------------------------
RFR_MODEL_SAVE_PATH_FORMAT = "./data/curated/hypertune_rfr_models/hypertune_rfr_models_{}_{}/"
RFR_COUNT_SAVE_PATH_FORMAT = "./data/curated/hypertune_rfr_models/count_{}_{}.txt"
PREDICTION_COL_NAME_FORMAT = "prediction_dollar_value_{}"
PREDICTION_VALUES_SAVE_FORMAT = "./data/curated/hypertune_rfr_models/predictions_{}_{}.txt"

RFR_NAME_MODEL_FORMAT = "rfr_model_{}_{}.py"
RFR_FILE_MODEL_FORMAT = """from ..constants import *
from ..read import *
from ..model_rfr import *

if __name__ == "__main__":
    spark = create_spark()
    train_df = read_train_data_rfr(spark, {}, {})
    fit_rfr(train_df, {}, {}, {}, {}, {}, {})
"""

RFR_NAME_PREDICT_FORMAT = "rfr_predict_{}_{}.py"
RFR_FILE_PREDICT_FORMAT = """from ..constants import *
from ..read import *
from ..model_rfr import *

if __name__ == "__main__":
    spark = create_spark()
    test_df = read_test_data(spark)
    test_df = test_df.sample(withReplacement=True, fraction=1.0)
"""
RFR_FILE_PREDICT_ADD_FORMAT = "    predict_rfr(test_df, {}, {}, {}, {}, {})\n"
RFR_FILE_EVAL_ADD_FORMAT = "    calculate_rmse(test_df, {}, {}, {}, {}, {})"

RFR_NAME_EVAL_FORMAT = "rfr_evaluate_{}_{}.py"
RFR_FILE_EVAL_FORMAT = """from ..constants import *
from ..read import *
from ..model_rfr import *

if __name__ == "__main__":
    spark = create_spark()
    test_df = read_test_data_rfr(spark)
    calculate_rmse(test_df, {}, {}, {}, {}, {})
"""

# -------------------------------------- NEURAL NET MODELS --------------------------------------

NEURALNET_NAME_EVAL_FORMAT = "neuralnet_evaluate_{}.py"
NEURALNET_INITIALIZE_NAME_FORMAT = "neuralnet_initialize_{}_{}.py"
NEURALNET_INITIALIZE_FILE_FORMAT = """from ..constants import *
from ..read import *
from ..model_neuralnet import *

if __name__ == "__main__":
    initialize_param_txt_file({}, {})
"""
NEURALNET_FILE_EVAL_FORMAT = """from ..constants import *
from ..read import *
from ..model_neuralnet import *

if __name__ == "__main__":
    spark = create_spark()
    train_df = read_train_data_neuralnet(spark).toPandas()
    test_df = read_test_data_neuralnet(spark)
    test_df = test_df.sample(withReplacement=True, fraction=1.0)
    test_df = test_df.toPandas()
    hyperparameters_neuralnet(train_df, test_df, {}, {}, {})
"""

# -------------------------------------- TIME SERIES --------------------------------------
TS_NAME_EVAL_FORMAT = "ts_evaluate_{}.py"
TS_INITIALIZE_NAME_FORMAT = "ts_initialize_{}_{}.py"
TS_INITIALIZE_FILE_FORMAT = """from ..constants import *
from ..read import *
from ..model_ts import *

if __name__ == "__main__":
    ts_initialize_param_txt_file({}, {})
"""
TS_FILE_EVAL_FORMAT = """from ..constants import *
from ..read import *
from ..model_ts import *
import warnings

warnings.simplefilter('ignore', ConvergenceWarning)

if __name__ == "__main__":
    spark = create_spark()
    train_df = read_train_data_ts(spark).toPandas()
    test_df = read_test_data_ts(spark)
    test_df = test_df.sample(withReplacement=True, fraction=1.0)
    test_df = test_df.toPandas()
    hyperparameters_ts(train_df, test_df, {}, {}, {})
"""

# -------------------------------------- FUNCTIONS --------------------------------------

def initialize_linreg_results(prefix:str="") -> None:
    """
    Initializes the text file that stores all the results
    - Parameters
        - prefix: Prefix for access from different directories
    - Returns
        - None, but saves a new text path used to save results
    """
    with open(prefix+LINREG_RMSE_SAVE_PATH, "w") as fp:
        fp.write("")
    return

def construct_linreg_hypertune_files(reg_param:int, elastic_net:int, model_idx:int,
                                     target_col:str=DOLLAR_VALUE,
                                     prefix:str="") -> None:
    """
    Constructs the files needed to calculate the RMSE for the given parameters
    using cross-validation
    - Parameters
        - reg_param  : Regularization Parameter
        - elastic_net: Elastic Net Parameter
        - model_idx  : Index used for saving different files
        - target_col : Column with target features
        - prefix     : Prefix used for access in different directories
    - Returns
        - None, but saves the files needed to run a trial of the linear regression cross-validation
    """
    # Construct the model script
    model_fname = LINREG_NAME_MODEL_FORMAT.format(str(model_idx))
    model_save_path = prefix+LINREG_MODEL_SAVE_PATH_FORMAT.format(model_idx)
    model_pyfile = LINREG_FILE_MODEL_FORMAT.format("\""+model_save_path+"\"", "\""+target_col+"\"",
                                                   str(elastic_net), str(reg_param))
    with open(prefix+HYPERTUNE_LINREG_PATH+model_fname, "w") as fp:
        fp.write(model_pyfile)
    
    # Now construct the validation files
    for i in range(NUM_VALIDATIONS):
        validation_fname = LINREG_NAME_EVAL_FORMAT.format(model_idx, i)
        validation_pyfile = LINREG_FILE_EVAL_FORMAT.format("\""+model_save_path+"\"", "\""+LINREG_RMSE_SAVE_PATH+"\"",
                                                           "\""+target_col+"\"")
        with open(prefix+HYPERTUNE_LINREG_PATH+validation_fname, "w") as fp:
            fp.write(validation_pyfile)
    return


def make_linreg_hypertune_files(prefix:str="") -> None:
    """
    Makes the specific scripts needed to run the linear regression
    hyperparameter tuning
    - Parameters
        - prefix: Used for access from other directories
    - Returns
        - None, but saves the needed files needed to run hypertuning
    """
    # Need this for storing paths
    i = 0

    # Initialize RMSE store file
    initialize_linreg_results()

    # Make the folders to store the models
    if not os.path.exists(prefix+LINREG_MODELS_PATH):
        os.makedirs(prefix+LINREG_MODELS_PATH)

    # Make the folder to store the script
    if not os.path.exists(prefix+HYPERTUNE_LINREG_PATH):
        os.makedirs(prefix+HYPERTUNE_LINREG_PATH)

    for reg_param in POSSIBLE_REG_PARAMS:
        for elastic_net in POSSIBLE_ELASTIC_NET_PARAMS:
            construct_linreg_hypertune_files(reg_param, elastic_net, i)
            i += 1
    
    # Now make the run file
    with open(prefix+RUN_HYPERTUNE_LINREG_TXT_PATH, "w") as fp:
        # fp.write(PYTHON_CONSOLE_FORMAT.format(MAKE_MODEL_SCRIPTS+"\n"))
        tuning_commands = []
        for i in range(len(POSSIBLE_ELASTIC_NET_PARAMS) * len(POSSIBLE_REG_PARAMS)):
            tuning_commands.append(LINREG_CONSOLE_FORMAT.format(LINREG_NAME_MODEL_FORMAT.format(str(i))[:REMOVE_PY_VAL]))
            for j in range(NUM_VALIDATIONS):
                tuning_commands.append(LINREG_CONSOLE_FORMAT.format(LINREG_NAME_EVAL_FORMAT.format(str(i), str(j))[:REMOVE_PY_VAL]))
        tuning_str = "\n".join(tuning_commands)+"\n"
        fp.write(tuning_str)
    # Now make the associated files to run it
    with open(prefix+RUN_HYPERTUNE_LINREG_SH_PATH, "w") as fp:
        fp.write(SHELL_FORMAT.format(RUN_HYPERTUNE_LINREG_TXT, RUN_HYPERTUNE_LINREG_TXT))
    with open(prefix+RUN_HYPERTUNE_LINREG_BATCH_PATH, "w") as fp:
        fp.write(SHELL_FORMAT.format(RUN_HYPERTUNE_LINREG_TXT, RUN_HYPERTUNE_LINREG_TXT))
    return

def initialize_rfr_results(prefix:str="") -> None:
    """
    Initializes the text file that stores all the results
    - Parameters
        - prefix: Prefix for access from different directories
    - Returns
        - None, but initializes the random forest RMSE file
    """
    with open(prefix+RFR_RMSE_SAVE_PATH, "w") as fp:
        fp.write("")
    return

def construct_rfr_hypertune_files(depth:int, num_trees:int, model_idx:int,
                                     target_col:str=DOLLAR_VALUE,
                                     prefix:str="") -> None:
    """
    Constructs the files needed to calculate the RMSE for the given parameters for a Random Forest
    - Parameters
        - depth     : Maximum depth trees in the regressor go
        - num_trees : Number of decision trees to employ
        - model_idx : Index used for saving different files
        - target_col: Column with target features
        - prefix    : Prefix used for access in different directories
    - Returns
        - None, but saves the files to run a trial for Random Forest Regression (RFR) hypertuning
    """
    # Construct the model script, number of models
    # depends on how many RFR trees we construct
    for i in range(0, MAX_FILES_RFR, TRAIN_BATCH_RFR):
        model_fname = RFR_NAME_MODEL_FORMAT.format(str(model_idx), str(i//TRAIN_BATCH_RFR))
        model_save_path = prefix+RFR_MODEL_SAVE_PATH_FORMAT.format(model_idx, str(i//TRAIN_BATCH_RFR))
        count_save_path = prefix+RFR_COUNT_SAVE_PATH_FORMAT.format(model_idx, str(i//TRAIN_BATCH_RFR))
        prediction_col_name = PREDICTION_COL_NAME_FORMAT.format(str(i//TRAIN_BATCH_RFR))
        model_pyfile = RFR_FILE_MODEL_FORMAT.format(i, i+TRAIN_BATCH_RFR,
                                                    "\""+model_save_path+"\"", 
                                                    "\""+count_save_path+"\"", 
                                                    "\""+target_col+"\"",
                                                    "\""+prediction_col_name+"\"",
                                                    str(depth), str(num_trees))
        with open(prefix+HYPERTUNE_RFR_PATH+model_fname, "w") as fp:
            fp.write(model_pyfile)
    
    # Now construct the validation files
    for i in range(NUM_VALIDATIONS):
        eval_fname = RFR_NAME_EVAL_FORMAT.format(model_idx, i)
        eval_pyfile = RFR_FILE_PREDICT_FORMAT
        with open(prefix+HYPERTUNE_RFR_PATH+eval_fname, "w") as fp:
            fp.write(eval_pyfile)
            # Now add the other lines in the file
            for j in range(0, MAX_FILES_RFR, TRAIN_BATCH_RFR):
                model_path = prefix+RFR_MODEL_SAVE_PATH_FORMAT.format(model_idx, str(j//TRAIN_BATCH_RFR))
                predictions_path = prefix+PREDICTION_VALUES_SAVE_FORMAT.format(model_idx, str(j//TRAIN_BATCH_RFR))
                prediction_col_name = PREDICTION_COL_NAME_FORMAT.format(str(j//TRAIN_BATCH_RFR))
                prediction_line = RFR_FILE_PREDICT_ADD_FORMAT.format("\""+model_path+"\"",
                                                               "\""+predictions_path+"\"",
                                                               "\""+target_col+"\"",
                                                               "\""+prediction_col_name+"\"",
                                                               "\""+prefix+"\"")
                fp.write(prediction_line)


            # Then we make the evaluation file
            prediction_paths = [prefix+PREDICTION_VALUES_SAVE_FORMAT.format(model_idx, str(j//TRAIN_BATCH_RFR))\
                                for j in range(0, MAX_FILES_RFR, TRAIN_BATCH_RFR)]
            count_paths = [prefix+RFR_COUNT_SAVE_PATH_FORMAT.format(model_idx, str(i//TRAIN_BATCH_RFR))\
                        for i in range(0, MAX_FILES_RFR, TRAIN_BATCH_RFR)]
            evaluation_line = RFR_FILE_EVAL_ADD_FORMAT.format("\""+DOLLAR_VALUE+"\"",
                                                            "\""+RFR_RMSE_SAVE_PATH+"\"",
                                                            str(prediction_paths),
                                                            str(count_paths),
                                                            "\""+prefix+"\"")
            fp.write(evaluation_line)
    return

def make_rfr_hypertune_files(target_col:str=DOLLAR_VALUE, prefix:str="") -> None:
    """
    Makes the specific scripts needed to run the random forest regression
    hyperparameter tuning
    - Parameters
        - target_col:Column with target features
        - prefix    : Used for access from other directories
    - Returns
        - None, but saves the files needed to run the whole RFR hypertuning
    """
    # Need this for storing paths
    i = 0

    # Initialize RMSE store file
    initialize_rfr_results()

    # Make the folders to store the models
    if not os.path.exists(prefix+RFR_MODELS_PATH):
        os.makedirs(prefix+RFR_MODELS_PATH)

    # Make the folder to store the script
    if not os.path.exists(prefix+HYPERTUNE_RFR_PATH):
        os.makedirs(prefix+HYPERTUNE_RFR_PATH)

    for depth in MAX_DEPTH:
        for trees in NUM_TREES:
            construct_rfr_hypertune_files(depth, trees, i, target_col)
            i += 1
    
    # Now make the run file
    with open(prefix+RUN_HYPERTUNE_RFR_TXT_PATH, "w") as fp:
        # fp.write(PYTHON_CONSOLE_FORMAT.format(MAKE_MODEL_SCRIPTS+"\n"))
        tuning_commands = []
        for i in range(len(NUM_TREES) * len(MAX_DEPTH)):
            for k in range(0, MAX_FILES_RFR, TRAIN_BATCH_RFR):
                tuning_commands.append(RFR_CONSOLE_FORMAT.format(RFR_NAME_MODEL_FORMAT.format(str(i), 
                                                                                              str(k//TRAIN_BATCH_RFR))[:REMOVE_PY_VAL]))
            # for j in range(NUM_VALIDATIONS):
            #     for k in range(0, MAX_FILES_RFR, TRAIN_BATCH_RFR):
            #         tuning_commands.append(RFR_CONSOLE_FORMAT.format(RFR_NAME_PREDICT_FORMAT.format(str(i), str(j), 
            #                                                                                         str(k//TRAIN_BATCH_RFR))[:REMOVE_PY_VAL]))
            for j in range(NUM_VALIDATIONS):
                # tuning_commands.append(RFR_CONSOLE_FORMAT.format(RFR_NAME_PREDICT_FORMAT.format(str(i), str(j))[:REMOVE_PY_VAL]))
                tuning_commands.append(RFR_CONSOLE_FORMAT.format(RFR_NAME_EVAL_FORMAT.format(str(i), str(j))[:REMOVE_PY_VAL]))
        tuning_str = "\n".join(tuning_commands)+"\n"
        fp.write(tuning_str)
    # Now make the associated files to run it
    with open(prefix+RUN_HYPERTUNE_RFR_SH_PATH, "w") as fp:
        fp.write(SHELL_FORMAT.format(RUN_HYPERTUNE_RFR_TXT, RUN_HYPERTUNE_RFR_TXT))
    with open(prefix+RUN_HYPERTUNE_RFR_BATCH_PATH, "w") as fp:
        fp.write(BATCH_FORMAT.format(RUN_HYPERTUNE_RFR_TXT, RUN_HYPERTUNE_RFR_TXT))
    return

def construct_ts_hypertune_files(model_idx:int, target_col:str=DOLLAR_VALUE,
                                     prefix:str="") -> None:
    """
    Constructs the files needed to calculate the RMSE for the given parameters for a Time Series (SARIMA)
    - Parameters
        - model_idx  : Index used for saving different files
        - target_col : Column with target features
        - prefix     : Used for access from other directories
    - Returns
        - None, but constructs the files to run one instance of cross validation of a SARIMA time series
    """

    # Make a script for each validation
    for i in range(NUM_VALIDATIONS):
        eval_fname = TS_NAME_EVAL_FORMAT.format(model_idx, i)
        eval_pyfile = TS_FILE_EVAL_FORMAT.format("\""+TS_RMSE_SAVE_PATH+"\"", "\""+target_col+"\"",
                                                 "\""+prefix+"\"")
        with open(prefix+HYPERTUNE_TS_PATH+eval_fname, "w") as fp:
            fp.write(eval_pyfile)
    return

def make_ts_hypertune_files(target_col:str=DOLLAR_VALUE, prefix:str="") -> None:
    """
    Makes the specific scripts needed to run the time series (SARIMA)
    hyperparameter tuning
    - Parameters
        - target_col: Column with target features
        - prefix    : Used for access from other directories
    - Returns
        - None, but makes all the files needed to run for the Time Series hypertuning
    """
    # Need this for storing paths
    i = 0

    # Initialize RMSE store file
    ts_initialize_param_txt_file(save_path=TS_RMSE_SAVE_PATH)

    # Make the folder to store the script
    if not os.path.exists(prefix+HYPERTUNE_TS_PATH):
        os.makedirs(prefix+HYPERTUNE_TS_PATH)

    for p in P_VALS:
        for q in Q_VALS:
            construct_ts_hypertune_files(p, q, i, target_col)
            i += 1

    # Now make the run file
    with open(RUN_HYPERTUNE_TS_TXT_PATH, "w") as fp:
        # fp.write(PYTHON_CONSOLE_FORMAT.format(MAKE_MODEL_SCRIPTS+"\n"))
        tuning_commands = []
        for j in range(len(P_VALS)* len(Q_VALS)):
            tuning_commands.append(TS_CONSOLE_FORMAT.format(TS_NAME_EVAL_FORMAT.format(str(j))[:REMOVE_PY_VAL]))
        tuning_str = "\n".join(tuning_commands)+"\n"
        fp.write(tuning_str)
    # Now make the associated files to run it
    with open(prefix+RUN_HYPERTUNE_TS_SH_PATH, "w") as fp:
        fp.write(SHELL_FORMAT.format(RUN_HYPERTUNE_TS_TXT, RUN_HYPERTUNE_TS_TXT))
    with open(prefix+RUN_HYPERTUNE_TS_BATCH_PATH, "w") as fp:
        fp.write(BATCH_FORMAT.format(RUN_HYPERTUNE_TS_TXT, RUN_HYPERTUNE_TS_TXT))
    return

def construct_neuralnet_hypertune_files(model_idx:int,
                                     target_col:str=DOLLAR_VALUE,
                                     prefix:str="") -> None:
    """
    Constructs the files needed to calculate the RMSE for the given parameters or a neural network
    - Parameters
        - model_idx: Index used for saving different files
        - target_col: Column with target features
        - prefix: Used for access from other directories
    - Returns
        - None, but makes the files needed for one instance of Neural Network cross validation
    """

    # Make a script for each validation
    for i in range(NUM_VALIDATIONS):
        eval_fname = NEURALNET_NAME_EVAL_FORMAT.format(model_idx, i)
        eval_pyfile = NEURALNET_FILE_EVAL_FORMAT.format("\""+NEURALNET_RMSE_SAVE_PATH+"\"", "\""+target_col+"\"",
                                                 "\""+prefix+"\"")
        with open(prefix+HYPERTUNE_NEURALNET_PATH+eval_fname, "w") as fp:
            fp.write(eval_pyfile)
    return

def make_neuralnet_hypertune_files(target_col:str=DOLLAR_VALUE, prefix:str="") -> None:
    """
    Makes the files needed to run the Neural Network hypertuning
    - Parameters
        - target_col: Target column to use
        - prefix    : Used for access from other directories
    - Returns
        - None, but makes the files needed for the whole Neural Network hypertuning
    """
    # Need this for storing paths
    i = 0

    # Initialize RMSE store file
    neuralnet_initialize_param_txt_file(save_path=NEURALNET_RMSE_SAVE_PATH)

    # Make the folder to store the script
    if not os.path.exists(prefix+HYPERTUNE_NEURALNET_PATH):
        os.makedirs(prefix+HYPERTUNE_NEURALNET_PATH)

    for size in BATCH_SIZES:
        for ep in EPOCHS:
            construct_neuralnet_hypertune_files(size, ep, i, target_col)
            i += 1

    # Now make the run file
    with open(RUN_HYPERTUNE_NEURALNET_TXT_PATH, "w") as fp:
        # fp.write(PYTHON_CONSOLE_FORMAT.format(MAKE_MODEL_SCRIPTS+"\n"))
        tuning_commands = []
        for j in range(len(EPOCHS)* len(BATCH_SIZES)):
            tuning_commands.append(NEURALNET_CONSOLE_FORMAT.format(NEURALNET_NAME_EVAL_FORMAT.format(str(j))[:REMOVE_PY_VAL]))
        tuning_str = "\n".join(tuning_commands)+"\n"
        fp.write(tuning_str)
    # Now make the associated files to run it
    with open(prefix+RUN_HYPERTUNE_NEURALNET_SH_PATH, "w") as fp:
        fp.write(SHELL_FORMAT.format(RUN_HYPERTUNE_NEURALNET_TXT, RUN_HYPERTUNE_NEURALNET_TXT))
    with open(prefix+RUN_HYPERTUNE_NEURALNET_BATCH_PATH, "w") as fp:
        fp.write(BATCH_FORMAT.format(RUN_HYPERTUNE_NEURALNET_TXT, RUN_HYPERTUNE_NEURALNET_TXT))
    return



if __name__ == "__main__":
    make_linreg_hypertune_files()
    make_rfr_hypertune_files()
    make_ts_hypertune_files()
    make_neuralnet_hypertune_files()