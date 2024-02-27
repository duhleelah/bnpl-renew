from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, DoubleType, LongType, TimestampNTZType, StringType, IntegerType, DateType
import os
from functools import reduce
from pyspark.sql import DataFrame


# CONSTANTS

# File Types
CSV = ".csv"
PARQUET = ".parquet"
EXCEL = ".xlsx"
ZIP = ".zip"
SHP = ".shp"
XLS = ".xls"
CPG = ".cpg"
DBF = ".dbf"
PRJ = ".prj"
SHX = ".shx"
TXT = ".txt"
BATCH = ".bat"
SHELL = ".sh"
JSON = ".json"
PNG = ".png"

# File Keywords
TRANSACTIONS = "transactions"
TRANSACTIONS_BATCH = "transactions_batch"
TRANSACTIONS_MERCHANTS = "transactions_merchants"
TRANSACTIONS_CONSUMERS = "transactions_consumers"
TRANSACTIONS_RAW_PARTITION = "transactions_raw_partition"
TRANSACTIONS_TRANSFORMED_PARTITION = "transactions_transform_partition"
TRANSACTIONS_WHOLE = "transactions_whole"
TRANSACTIONS_OHE_BIG = "transactions_ohe_big"
TRANSACTIONS_OHE_SMALL= "transactions_ohe_small"
TRANSACTIONS_DISTINCT = "transactions_distinct"
SNAPSHOT = "snapshot"
POPULATIONS_DATA = "populations_data"

# URLs
AU_SA2_SHAPE_FILE_URL = "https://www.abs.gov.au/statistics/standards/australian-statistical-geography-standard-asgs-edition-3/jul2021-jun2026/access-and-downloads/digital-boundary-files/SA2_2021_AUST_SHP_GDA2020.zip"
EARNING_INFO_BY_SA2_URL = "https://www.abs.gov.au/statistics/labour/earnings-and-working-conditions/personal-income-australia/2015-16-2019-20/6524055002_DO001.xlsx"
POPULATION_INFO_BY_AGE_BY_SEX_BY_SA2_URL = "https://www.abs.gov.au/statistics/people/population/regional-population-age-and-sex/2021/32350DS0001_2021.xlsx"
CORRESPONDING_SA2_TO_POSTCODE_URL = "https://www.abs.gov.au/AUSSTATS/subscriber.nsf/log?openagent&1270055006_CG_POSTCODE_2011_SA2_2011.zip&1270.0.55.006&Data%20Cubes&70A3CE8A2E6F9A6BCA257A29001979B2&0&July%202011&27.06.2012&Latest"
POSTCODE_TO_2016_SA2_URL = "https://www.matthewproctor.com/Content/postcodes/australian_postcodes.csv"
SA2_2016_TO_SA2_2021_URL = "https://www.abs.gov.au/statistics/standards/australian-statistical-geography-standard-asgs-edition-3/jul2021-jun2026/access-and-downloads/correspondences/CG_SA2_2016_SA2_2021.csv"

# Data Directory
DATA_DIR = "./data/"
SCRIPTS_DIR = "./scripts/"
PLOTS_DIR = "./plots/"

# Raw Directories
RAW_DIR = f"{DATA_DIR}raw/"
RAW_CONSUMER_FRAUD_PATH = f"{RAW_DIR}consumer_fraud_probability{CSV}"
RAW_CONSUMER_USER_DETAILS_PATH = f"{RAW_DIR}consumer_user_details{PARQUET}"
RAW_MERCHANT_FRAUD_PATH = f"{RAW_DIR}merchant_fraud_probability{CSV}"
RAW_TBL_CONSUMER_PATH = f"{RAW_DIR}tbl_consumer{CSV}"
RAW_TBL_MERCHANTS_PATH = f"{RAW_DIR}tbl_merchants{PARQUET}"
RAW_TRANSACTIONS_PATH = f"{RAW_DIR}raw_transactions/"

# Raw Directories for external data
RAW_SA2_SHAPE_FILE_PATH = f"{RAW_DIR}sa2_shape_file{SHP}"
RAW_EARNING_INFO_BY_SA2_PATH = f"{RAW_DIR}earning_info_by_sa2{CSV}"
RAW_MALE_POPULATION_INFO_BY_AGE_BY_SEX_BY_SA2 = f"{RAW_DIR}male_population_info_by_age_by_sex_by_sa2{CSV}"
RAW_FEMALE_POPULATION_INFO_BY_AGE_BY_SEX_BY_SA2 = f"{RAW_DIR}female_population_info_by_age_by_sex_by_sa2{CSV}"
RAW_CORRESPONDING_SA2_TO_POSTCODE_PATH = f"{RAW_DIR}corresponding_sa2_to_postcode{CSV}"
RAW_POSTCODE_TO_2016_SA2_PATH = f"{RAW_DIR}postcode_to_2016_sa2{CSV}"
RAW_SA2_2016_TO_SA2_2021_PATH = f"{RAW_DIR}sa2_2016_to_sa2_2021{CSV}"

# Curated Directories
CURATED_DIR = f"{DATA_DIR}curated/"
CURATED_CONSUMER_FRAUD_PATH = f"{CURATED_DIR}consumer_fraud_probability{CSV}"
CURATED_CONSUMER_USER_DETAILS_PATH = f"{CURATED_DIR}consumer_user_details{PARQUET}"
CURATED_MERCHANT_FRAUD_PATH = f"{CURATED_DIR}merchant_fraud_probability{CSV}"
CURATED_TBL_CONSUMER_PATH = f"{CURATED_DIR}tbl_consumer{CSV}"
CURATED_TBL_MERCHANTS_PATH = f"{CURATED_DIR}tbl_merchants{PARQUET}"

# Curated Directories for external data
CURATED_SA2_SHAPE_FILE_PATH = f"{CURATED_DIR}sa2_shape_file{SHP}"
CURATED_EARNING_INFO_BY_SA2_PATH = f"{CURATED_DIR}earning_info_by_sa2{CSV}"
CURATED_MALE_POPULATION_INFO_BY_AGE_BY_SEX_BY_SA2 = f"{CURATED_DIR}male_population_info_by_age_by_sex_by_sa2{CSV}"
CURATED_FEMALE_POPULATION_INFO_BY_AGE_BY_SEX_BY_SA2 = f"{CURATED_DIR}female_population_info_by_age_by_sex_by_sa2{CSV}"
CURATED_CORRESPONDING_SA2_TO_POSTCODE_PATH = f"{CURATED_DIR}corresponding_sa2_to_postcode{CSV}"
CURATED_POSTCODE_TO_SA2_PATH = f"{CURATED_DIR}postcode_to_sa2{CSV}"
CURATED_EXTERNAL_JOINED_DATA = f"{CURATED_DIR}external_joined_data{CSV}"

# Curated Directories for joined data
CURATED_EXTERNAL_JOIN_PATH = f"{CURATED_DIR}external_joined_data{CSV}"
CURATED_AVERAGE_DOLLAR_SPENT_PER_USER_PATH = f"{CURATED_DIR}average_dollar_spent_per_user{CSV}"
CURATED_CONSUMER_JOINED_DATA = f"{CURATED_DIR}consumer_joined_data{PARQUET}"
CURATED_MERCHANT_JOINED_DATA = f"{CURATED_DIR}merchant_joined_data{PARQUET}"
CURATED_CONSUMER_EXTERNAL_JOIN_PATH = f"{CURATED_DIR}consumer_external_join{PARQUET}"
CURATED_ALLOCATED_SA2_CONSUMER_PATH = f"./data/curated/consumer_data_filtered{PARQUET}"
CURATED_CONSUMER_EXTERNAL_JOIN_AGE_ALLOCATED_PATH = f"{CURATED_DIR}consumer_external_join_age_allocated{PARQUET}"
CURATED_TRANSACTIONS_MERCHANTS_PATH = f"{CURATED_DIR}transactions_merchants/"
CURATED_TRANSACTIONS_ALL_PATH = f"{CURATED_DIR}transactions_all/"
CURATED_TRANSACTIONS_ALL_BATCHED_PATH = f"{CURATED_DIR}transactions_all_batched/"
CURATED_TRANSACTIONS_CURATED_PATH = f"{CURATED_DIR}transactions_curated/"
CURATED_TRANSACTIONS_OHE_PATH = f"{CURATED_DIR}transactions_ohe/"
CURATED_TRANSACTIONS_OHE_BIG_PATH = f"{CURATED_DIR}transactions_ohe_big/"

CURATED_MERCHANT_ABN_DISTINCTS_PATH = f"{CURATED_DIR}distincts_merchant_abn{TXT}"
CURATED_INDUSTRY_TAGS_DISTINCTS_PATH = f"{CURATED_DIR}distincts_industry_tags{TXT}"
CURATED_REVENUE_LEVEL_DISTINCTS_PATH = f"{CURATED_DIR}distincts_revenue_level{TXT}"
CURATED_USER_ID_DISTINCTS_PATH = f"{CURATED_DIR}distincts_user_id{TXT}"
CURATED_SA2_CODE_DISTINCTS_PATH = f"{CURATED_DIR}distincts_sa2_code{TXT}"
CURATED_ORDER_DAY_OF_MONTH_DISTINCTS_PATH = f"{CURATED_DIR}distincts_order_day_of_month{TXT}"
CURATED_ORDER_DAY_OF_WEEK_DISTINCTS_PATH = f"{CURATED_DIR}distincts_order_day_of_week{TXT}"
CURATED_ORDER_MONTH_DISTINCTS_PATH = f"{CURATED_DIR}distincts_order_month{TXT}"
CURATED_ORDER_YEAR_DISTINCTS_PATH = f"{CURATED_DIR}distincts_order_year{TXT}"
CURATED_GENDER_DISTINCTS_PATH = f"{CURATED_DIR}distincts_gender{TXT}"

CURATED_SA2_CODE_DICT_PATH = f"{CURATED_DIR}dict_sa2_code{JSON}"
CURATED_USER_ID_DICT_PATH = f"{CURATED_DIR}dict_user_id{JSON}"
CURATED_MERCHANT_ABN_DICT_PATH = f"{CURATED_DIR}dict_merchant_abn{JSON}"
CURATED_MALE_AGES_DICT_PATH = f"{CURATED_DIR}male_ages{JSON}"
CURATED_FEMALE_AGES_DICT_PATH = f"{CURATED_DIR}female_ages{JSON}"

# Table Directories
TABLES_DIR = f"{DATA_DIR}tables/"
CONSUMER_FRAUD_PATH = f"{TABLES_DIR}consumer_fraud_probability{CSV}"
CONSUMER_USER_DETAILS_PATH = f"{TABLES_DIR}consumer_user_details{PARQUET}"
MERCHANT_FRAUD_PATH = f"{TABLES_DIR}merchant_fraud_probability{CSV}"
TBL_CONSUMER_PATH = f"{TABLES_DIR}tbl_consumer{CSV}"
TBL_MERCHANTS_PATH = f"{TABLES_DIR}tbl_merchants{PARQUET}"
INDUSTRY_MAPPED = f"{TABLES_DIR}mapped_industry{CSV}"
CURATED_AVERAGE_AND_MEDIAN_DOLLAR_PER_TAG_PATH = f"{TABLES_DIR}average_and_median_dollar_per_tag{CSV}"
CURATED_NUMBER_OF_TRANSACTIONS_PATH = f"{TABLES_DIR}number_of_transactions{CSV}"
CURATED_AVERAGE_AND_MEDIAN_FRAUD_PROB_PATH = f"{TABLES_DIR}average_and_median_fraud_prob{CSV}"
CURATED_AVERAGE_EARNING_PATH = f"{TABLES_DIR}average_earning{CSV}"
CURATED_TOP3_AVERAGE_AND_MEDIAN_DOLLAR_PER_TAG_PATH = f"{TABLES_DIR}top3_average_and_median_dollar_per_tag{CSV}"
CURATED_TOP3_NUMBER_OF_TRANSACTIONS_PATH = f"{TABLES_DIR}top3_number_of_transactions{CSV}"
CURATED_TOP3_AVERAGE_AND_MEDIAN_FRAUD_PROB_PATH = f"{TABLES_DIR}top3_average_and_median_fraud_probability{CSV}"
CURATED_TOP3_AVERAGE_EARNING_PATH = f"{TABLES_DIR}top3_average_earning{CSV}"

INDUSTRY_MAPPING_PATH = f"{TABLES_DIR}mapped_industry{CSV}"
REVENUE_A_PATH = f"{TABLES_DIR}revenue_a{CSV}"
REVENUE_B_PATH = f"{TABLES_DIR}revenue_b{CSV}"
REVENUE_C_PATH = f"{TABLES_DIR}revenue_c{CSV}"
REVENUE_D_PATH = f"{TABLES_DIR}revenue_d{CSV}"
REVENUE_E_PATH = f"{TABLES_DIR}revenue_e{CSV}"
CURATED_TAKERATE_PATH = f"{TABLES_DIR}takerate_mean_median{CSV}"
SORTED_INDUSTRY_PATH = f"{TABLES_DIR}sorted_rank_df{CSV}"
TOP3_INDUSTRY_REVENUE_TAKERATE_PATH = f"{TABLES_DIR}top3_revenue_takerate{CSV}"
TOP_100_MERCHANTS_PATH = f"{TABLES_DIR}top_100_merchants{CSV}"

# Table Directories for external data
ZIP_SA2_SHAPE_FILE_PATH = f"{TABLES_DIR}sa2_shape_file{ZIP}"
SA2_SHAPE_FILE_PATH = f"{TABLES_DIR}SA2_2021_AUST_GDA2020{SHP}"
EARNING_INFO_BY_SA2_PATH = f"{TABLES_DIR}earning_info_by_sa2{EXCEL}"
POPULATION_INFO_BY_AGE_BY_SEX_BY_SA2_PATH = f"{TABLES_DIR}population_info_by_age_by_sa2_by_sex{EXCEL}"
ZIP_CORRESPONDING_SA2_TO_POSTCODE_PATH = f"{TABLES_DIR}corresponding_sa2_to_postcode{ZIP}"
CORRESPONDING_SA2_TO_POSTCODE_PATH = f"{TABLES_DIR}1270055006_CG_POSTCODE_2011_SA2_2011{XLS}"
POSTCODE_TO_2016_SA2_PATH = f"{TABLES_DIR}postcode_to_2016_sa2{CSV}"
SA2_2016_TO_SA2_2021_PATH = f"{TABLES_DIR}sa2_2016_to_sa2_2021{CSV}"

# Plot Directories
REVENUE_A_PLOT_PATH = f"{PLOTS_DIR}industry_dist_revenue_a{PNG}"
REVENUE_B_PLOT_PATH = f"{PLOTS_DIR}industry_dist_revenue_b{PNG}"
REVENUE_C_PLOT_PATH = f"{PLOTS_DIR}industry_dist_revenue_c{PNG}"
REVENUE_D_PLOT_PATH = f"{PLOTS_DIR}industry_dist_revenue_d{PNG}"
REVENUE_E_PLOT_PATH = f"{PLOTS_DIR}industry_dist_revenue_e{PNG}"
TOP_THREE_INDUSTRY_TAKERATE_PATH = f"{PLOTS_DIR}takerate_dist_top_three_industry{PNG}"

# Script Folders
HYPERTUNE_LINREG_PATH = f"{SCRIPTS_DIR}hypertune_linreg/"
HYPERTUNE_RFR_PATH = f"{SCRIPTS_DIR}hypertune_rfr/"
HYPERTUNE_TS_PATH = f"{SCRIPTS_DIR}hypertune_ts/"
HYPERTUNE_NEURALNET_PATH = f"{SCRIPTS_DIR}hypertune_neuralnet/"

# Model and Results Folders
LINREG_MODELS_PATH = f"{CURATED_DIR}hypertune_linreg_models/"
LINREG_RMSE_SAVE_PATH = f"{CURATED_DIR}hypertune_linreg_results.txt"

RFR_MODELS_PATH = f"{CURATED_DIR}hypertune_rfr_models/"
RFR_RMSE_SAVE_PATH = f"{CURATED_DIR}hypertune_rfr_results.txt"

TS_RMSE_SAVE_PATH = f"{CURATED_DIR}hypertune_ts_results.txt"
NEURALNET_RMSE_SAVE_PATH = "{CURATED_DIR}hypertune_neuralnet_results.txt"

# Text, Bash and Shell Scripts
RUN_SCRIPT_TXT_PATH = f"./run_files{TXT}"
RUN_SCRIPT_SH_PATH = f"./run_files{SHELL}"
RUN_SCRIPT_BATCH_PATH = f"./run_files{BATCH}"
RUN_FILES_TXT = f"run_files{TXT}"

RUN_SCRIPT_2_TXT_PATH = f"./run_files_2{TXT}"
RUN_SCRIPT_2_SH_PATH = f"./run_files_2{SHELL}"
RUN_SCRIPT_2_BATCH_PATH = f"./run_files_2{BATCH}"
RUN_FILES_2_TXT = f"run_files_2{TXT}"

RUN_SCRIPT_3_TXT_PATH = f"./run_files_3{TXT}"
RUN_SCRIPT_3_SH_PATH = f"./run_files_3{SHELL}"
RUN_SCRIPT_3_BATCH_PATH = f"./run_files_3{BATCH}"
RUN_FILES_3_TXT = f"run_files_3{TXT}"

RUN_HYPERTUNE_LINREG_TXT_PATH = f"./run_hypertune_linreg{TXT}"
RUN_HYPERTUNE_LINREG_SH_PATH = f"./run_hypertune_linreg{SHELL}"
RUN_HYPERTUNE_LINREG_BATCH_PATH = f"./run_hypertune_linreg{BATCH}"
RUN_HYPERTUNE_LINREG_TXT =  f"run_hypertune_linreg{TXT}"

RUN_HYPERTUNE_RFR_TXT_PATH = f"./run_hypertune_rfr{TXT}"
RUN_HYPERTUNE_RFR_SH_PATH = f"./run_hypertune_rfr{SHELL}"
RUN_HYPERTUNE_RFR_BATCH_PATH = f"./run_hypertune_rfr{BATCH}"
RUN_HYPERTUNE_RFR_TXT =  f"run_hypertune_rfr{TXT}"

RUN_HYPERTUNE_TS_TXT_PATH = f"./run_hypertune_ts{TXT}"
RUN_HYPERTUNE_TS_SH_PATH = f"./run_hypertune_ts{SHELL}"
RUN_HYPERTUNE_TS_BATCH_PATH = f"./run_hypertune_ts{BATCH}"
RUN_HYPERTUNE_TS_TXT =  f"run_hypertune_ts{TXT}"

RUN_HYPERTUNE_NEURALNET_TXT_PATH = f"./run_hypertune_neuralnet{TXT}"
RUN_HYPERTUNE_NEURALNET_SH_PATH = f"./run_hypertune_neuralnet{SHELL}"
RUN_HYPERTUNE_NEURALNET_BATCH_PATH = f"./run_hypertune_neuralnet{BATCH}"
RUN_HYPERTUNE_NEURALNET_TXT =  f"run_hypertune_neuralnet{TXT}"

DELETE_PARTITION_TXT_PATH= f"./delete_partitions{TXT}"
DELETE_PARTITION_SH_PATH = f"./delete_partitions{SHELL}"
DELETE_PARTITION_BATCH_PATH = f"./delete_partitions{BATCH}"
DELETE_PARTITION_TEXT = f"delete_partitions{TXT}"

# Plots Directories
AVERAGE_DOLLAR_VALUE_BY_STATE_PLOT_PATH = f"{PLOTS_DIR}average_dollar_value_by_state{PNG}"
CUSTOMER_LOYALTY_BY_SA2_PLOT_PATH = f"{PLOTS_DIR}customer_loyalty_by_sa2{PNG}"
CUSTOMER_LOYALTY_BY_STATE_PLOT_PATH = f"{PLOTS_DIR}customer_loyalty_by_state{PNG}"
CONSUMER_COUNT_BY_STATE_PLOT_PATH = f"{PLOTS_DIR}consumer_count_by_state{PNG}"
CONSUMER_PER_TRANSACTION_PER_STATE_PLOT_PATH = f"{PLOTS_DIR}consumer_per_transaction_per_state{PNG}"
SA_COUNT_BY_STATE_PIE_PLOT_PATH = f"{PLOTS_DIR}sa2_count_by_state_pie{PNG}"
SEX_RATIO_COMPARISON_PLOT_PATH = f"{PLOTS_DIR}sex_ratio_comparison{PNG}"
SEX_RATIO_AND_CUSTOMER_COUNT_IN_TOP100_SA2_PLOT_PATH = f"{PLOTS_DIR}sex_ratio_and_customer_count_in_top100_sa2{PNG}"
SPENT_DOLLAR_BY_SA2_PLOT_PATH = f"{PLOTS_DIR}spent_dollar_by_sa2{PNG}"
SPENT_DOLLAR_BY_STATE_PLOT_PATH = f"{PLOTS_DIR}spent_dollar_by_state{PNG}"
NUMBER_OF_MERCHANT_MADE_TRANSACTION_WITH_BY_SA2_PLOT_PATH = f"{PLOTS_DIR}number_of_merchant_made_transaction_by_sa2{PNG}"
NUMBER_OF_MERCHANT_MADE_TRANSACTION_WITH_BY_STATE_PLOT_PATH = f"{PLOTS_DIR}number_of_merchant_made_transaction_by_state{PNG}"
EARNING_MEAN_TREND_PLOT_PATH = f"{PLOTS_DIR}earning_mean_trend{PNG}"
EARNING_MEDIAN_TREND_PLOT_PATH = f"{PLOTS_DIR}earning_median_trend{PNG}"
EARNING_SUM_TREND_PLOT_PATH = f"{PLOTS_DIR}earning_sum_trend{PNG}"
EARNERS_TREND_PLOT_PATH = f"{PLOTS_DIR}earners_trend{PNG}"


# TRANSACTIONS_SNAPSHOTS = [f"{TABLES_DIR}{fname}" for fname in os.listdir(TABLES_DIR) if TRANSACTIONS in fname]

# Spark Parameters
SPARK_APP_NAME = "MAST30034 Project 2"
SPARK_EAGER_VAL = "spark.sql.repl.eagerEval.enabled"
SPARK_EAGER_VAL_SET = True
SPARK_CACHE_METADATA = "spark.sql.parquet.cacheMetadata"
SPARK_CACHE_METADATA_SET = "true"
SPARK_TIMEZONE = "spark.sql.session.timeZone"
SPARK_TIMEZONE_SET = "Etc/UTC"
SPARK_DRIVER_MEMORY = "spark.driver.memory"
SPARK_DRIVER_MEMORY_SET = "32g"
SPARK_AUTOBROADCAST_THRESHOLD = "spark.sql.autoBroadcastJoinThreshold"
SPARK_AUTOBROADCAST_THRESHOLD_SET = "-1"
SPARK_EXECUTOR_MEM_OVERHEAD = 'spark.executor.memoryOverhead'
SPARK_EXECUTOR_MEM_OVERHEAD_SET = '1500'

# Some functions
def create_spark() -> SparkSession:
    """
    Create a spark session
    - Parameters:
        - None
    - Returns:
        - SparkSession
    """
    spark =(
        SparkSession.builder.appName(SPARK_APP_NAME)
        .config(SPARK_EAGER_VAL, SPARK_EAGER_VAL_SET) 
        .config(SPARK_CACHE_METADATA, SPARK_CACHE_METADATA_SET)
        .config(SPARK_TIMEZONE, SPARK_TIMEZONE_SET)
        .config(SPARK_DRIVER_MEMORY, SPARK_DRIVER_MEMORY_SET)
        .config(SPARK_AUTOBROADCAST_THRESHOLD, SPARK_AUTOBROADCAST_THRESHOLD_SET)
        .config(SPARK_EXECUTOR_MEM_OVERHEAD, SPARK_EXECUTOR_MEM_OVERHEAD_SET)
        .getOrCreate()
    )
    return spark 

def get_transactions_length(folder:str, is_nested:bool=True, prefix:str="") -> int:
    """
    Gets the total number of transactions (order files) from a given folder
    - Parameters:
        - folder: The folder name for the transactions
        - is_nested: Whether the folder is nested or not
        - prefix: The prefix of the folder
    - Returns:
        - int: The total number of transactions
    """
    if is_nested:
        transaction_folders = [f"{prefix}{folder}{fname}" for fname in os.listdir(prefix+folder) \
                        if TRANSACTIONS in fname and SNAPSHOT in fname]
        transaction_folders_len = sum([len([fname for fname in os.listdir(folder_name) if ORDER_DATETIME in fname]) \
                                    for folder_name in transaction_folders])
    else:
        transaction_folders_len = len([fname for fname in os.listdir(prefix+folder) if ORDER_DATETIME in fname])
    return transaction_folders_len

def union_all(*dfs):
    """
    Union all the dataframes
    - Parameters:
        - *dfs: The dataframes to be unioned
    - Returns:
        - DataFrame: The unioned dataframe
    """
    return reduce(DataFrame.unionAll, dfs)

# Function Keywords
OVERWRITE = "overwrite"
INNER_JOIN = "inner"
LEFT_JOIN = "left"
RIGHT_JOIN = "right"
OUTER_JOIN = "outer"

# Column Names
USER_ID = "user_id"
CONSUMER_ID = "consumer_id"
CUSTOMER_ID = "customer_id"
ORDER_ID = "order_id"
ORDER_DATETIME = "order_datetime"
ORDER_YEAR = "order_year"
ORDER_MONTH = "order_month"
ORDER_DAY_OF_MONTH = "order_day_of_month"
ORDER_DAY_OF_WEEK = "order_day_of_week"
FRAUD_PROBABILITY = "fraud_probability"
MERCHANT_ABN = "merchant_abn"
NAME = "name"
ADDRESS = "address"
STATE = "state"
POSTCODE = "postcode"
GENDER = "gender"
TAGS = "tags"
DOLLAR_VALUE = "dollar_value"
INDUSTRY_TAGS = "industry_tags"
REVENUE_LEVEL = "revenue_level"
TAKE_RATE = "take_rate"
NUM_MERCHANTS = "num_merchants"
MERCHANT_FRAUD_PROB = "merchant_fraud_probability"
CONSUMER_FRAUD_PROB = "consumer_fraud_probability"
PREDICTED_DOLLARS = "predicted_dollar_value"
RESIDUALS = "residuals"
MAPPED_INDUSTRY = "MappedIndustry"

NUM_TRANSACTIONS = "number_of_transactions"
MAPPED_INDUSTRIES = "MappedIndustry"

# External Column Names
ALLOCATED_AGES = "allocated_ages"
FEATURES = "features"
SA2_CODE = "sa2_code"
SA2_NAME = "sa2_name"
SA2_NAME_X = "sa2_name_x"
SA2_NAME_Y = "sa2_name_y"
SA2_CODE_X = "sa2_code_x"
SA2_CODE_Y = "sa2_code_y"
STATE_NAME_X = "state_name_x"
STATE_NAME_Y = "state_name_y"
GEOMETRY = "geometry"
STE_NAME = "state_name"
STATE_CONVERT = {
    "Northern Territory" : "NT",
    "Australian Capital Territory" : "ACT",
    "South Australia" : "SA",
    "Tasmania" : "TAS",
    "Western Australia": "WA",
    "Queensland": "QLD",
    "Victoria": "VIC",
    "New South Wales" : "NSW"
}
EARNERS2015_TO_2016 = "earners2015-2016"
EARNERS2016_TO_2017 = "earners2016-2017"
EARNERS2017_TO_2018 = "earners2017-2018"
EARNERS2018_TO_2019 = "earners2018-2019"
EARNERS2019_TO_2020 = "earners2019-2020"
MEDIAN_AGE2015_TO_2016 = "median_age2015-2016"
MEDIAN_AGE2016_TO_2017 = "median_age2016-2017"
MEDIAN_AGE2017_TO_2018 = "median_age2017-2018"
MEDIAN_AGE2018_TO_2019 = "median_age2018-2019"
MEDIAN_AGE2019_TO_2020 = "median_age2019-2020"
EARNINGSUM2015_TO_2016 = "earningsum2015-2016"
EARNINGSUM2016_TO_2017 = "earningsum2016-2017"
EARNINGSUM2017_TO_2018 = "earningsum2017-2018"
EARNINGSUM2018_TO_2019 = "earningsum2018-2019"
EARNINGSUM2019_TO_2020 = "earningsum2019-2020"
EARNINGMEDIAN2015_TO_2016 = "earningmedian2015-2016"
EARNINGMEDIAN2016_TO_2017 = "earningmedian2016-2017"
EARNINGMEDIAN2017_TO_2018 = "earningmedian2017-2018"
EARNINGMEDIAN2018_TO_2019 = "earningmedian2018-2019"
EARNINGMEDIAN2019_TO_2020 = "earningmedian2019-2020"
EARININGMEAN2015_TO_2016 = "eariningmean2015-2016"
EARININGMEAN2016_TO_2017 = "eariningmean2016-2017"
EARININGMEAN2017_TO_2018 = "eariningmean2017-2018"
EARININGMEAN2018_TO_2019 = "eariningmean2018-2019"
EARININGMEAN2019_TO_2020 = "eariningmean2019-2020"
STATE_NAME = "state_name"
AGE0_TO_4 = "age0-4"
AGE5_TO_9 = "age5-9"
AGE10_TO_14 = "age10-14"
AGE15_TO_19 = "age15-19"
AGE20_TO_24 = "age20-24"
AGE25_TO_29 = "age25-29"
AGE30_TO_34 = "age30-34"
AGE35_TO_39 = "age35-39"
AGE40_TO_44 = "age40-44"
AGE45_TO_49 = "age45-49"
AGE50_TO_54 = "age50-54"
AGE55_TO_59 = "age55-59"
AGE60_TO_64 = "age60-64"
AGE65_TO_69 = "age65-69"
AGE70_TO_74 = "age70-74"
AGE75_TO_79 = "age75-79"
AGE80_TO_84 = "age80-84"
AGE85_AND_OVER = "age85_and_over"
TOTAL_MALES = "total_males"
TOTAL_FEMALES = "total_females"
POSTCODE = "postcode"
RATIO = "ratio"
MALE_PREFIX = "male_"
FEMALE_PREFIX = "female_"
MERCHANT_PREFIX = "merchant_"
CONSUMER_PREFIX = "consumer_"
AVERAGE_SUFFIX = '_avg'
MEDIAN_SUFFIX = '_median'
SA2_MAINCODE_2016 = "sa2_maincode_2016"
SA2_CODE_2016 = "sa2_code_2016"
SA2_CODE_2021 = "sa2_code_2021"

# Column Casing and Schema
# Consumer Fraud
CONSUMER_FRAUD_COLS_DICT = {
    "user_id": USER_ID,
    "order_datetime": ORDER_DATETIME,
    "fraud_probability": FRAUD_PROBABILITY
}

CONSUMER_FRAUD_COLS_SCHEMA = StructType([
    StructField(USER_ID,
                LongType(), True),
    StructField(ORDER_DATETIME,
                StringType(), True),
    StructField(FRAUD_PROBABILITY,
                DoubleType(), True)
])

# Consumer User Details 
CONSUMER_USER_DETAILS_COLS_DICT = {
    "user_id": USER_ID,
    "consumer_id": CONSUMER_ID
}

CONSUMER_USER_DETAILS_COLS_SCHEMA = StructType([
    StructField(USER_ID,
                LongType(), True),
    StructField(CONSUMER_ID,
                LongType(), True)
])

# Merchant Fraud
MERCHANT_FRAUD_COLS_DICT = {
    "merchant_abn": MERCHANT_ABN,
    "order_datetime": ORDER_DATETIME,
    "fraud_probability": FRAUD_PROBABILITY
}

MERCHANT_FRAUD_COLS_SCHEMA = StructType([
    StructField(MERCHANT_ABN,
                LongType(), True),
    StructField(ORDER_DATETIME,
                StringType(), True),
    StructField(FRAUD_PROBABILITY,
                DoubleType(), True)
])

# TBL Consumer
TBL_CONSUMER_COLS_DICT = {
    "name" : NAME,
    "address": ADDRESS,
    "state": STATE,
    "postcode": POSTCODE,
    "gender": GENDER,
    "consumer_id": CONSUMER_ID
}

TBL_CONSUMER_COLS_SCHEMA = StructType([
    StructField(NAME,
                StringType(), True),
    StructField(ADDRESS,
                StringType(), True),
    StructField(STATE,
                StringType(), True),
    StructField(POSTCODE,
                IntegerType(), True),
    StructField(GENDER,
                StringType(), True),
    StructField(CONSUMER_ID,
                LongType(), True),
])

# TBL Merchants
TBL_MERCHANTS_COLS_DICT = {
    "name": NAME,
    "tags": TAGS,
    "merchant_abn": MERCHANT_ABN
}

TBL_MERCHANTS_COLS_SCHEMA = StructType([
    StructField(NAME,
                StringType(), True),
    StructField(TAGS,
                StringType(), True),
    StructField(MERCHANT_ABN,
                LongType(), True)
])

# Transaction Data
TRANSACTIONS_COLS_DICT = {
    "user_id" : USER_ID,
    "merchant_abn": MERCHANT_ABN,
    "dollar_value": DOLLAR_VALUE,
    "order_id": ORDER_ID
}

TRANSACTIONS_COLS_SCHEMA = StructType([
    StructField(USER_ID,
                LongType(), True),
    StructField(MERCHANT_ABN,
                LongType(), True),
    StructField(DOLLAR_VALUE,
                DoubleType(), True),
    StructField(ORDER_ID,
                StringType(), True)
])

# External Data
SHAPE_COLS_DICT = {
    "SA2_CODE21": SA2_CODE,
    "SA2_NAME21": SA2_NAME, 
    "STE_NAME21": STATE,
    "geometry": GEOMETRY
}
                
EARNING_INFO_COLS_DICT = {
    "SA2": SA2_CODE,
    "SA2 NAME": SA2_NAME,
    "2015-16": EARNERS2015_TO_2016,
    "2016-17": EARNERS2016_TO_2017,
    "2017-18": EARNERS2017_TO_2018,
    "2018-19": EARNERS2018_TO_2019,
    "2019-20": EARNERS2019_TO_2020,
    "2015-16.1": MEDIAN_AGE2015_TO_2016,
    "2016-17.1": MEDIAN_AGE2016_TO_2017,
    "2017-18.1": MEDIAN_AGE2017_TO_2018,
    "2018-19.1": MEDIAN_AGE2018_TO_2019,
    "2019-20.1": MEDIAN_AGE2019_TO_2020,
    "2015-16.2": EARNINGSUM2015_TO_2016,
    "2016-17.2": EARNINGSUM2016_TO_2017,
    "2017-18.2": EARNINGSUM2017_TO_2018,
    "2018-19.2": EARNINGSUM2018_TO_2019,
    "2019-20.2": EARNINGSUM2019_TO_2020,
    "2015-16.3": EARNINGMEDIAN2015_TO_2016,
    "2016-17.3": EARNINGMEDIAN2016_TO_2017,
    "2017-18.3": EARNINGMEDIAN2017_TO_2018,
    "2018-19.3": EARNINGMEDIAN2018_TO_2019,
    "2019-20.3": EARNINGMEDIAN2019_TO_2020,
    "2015-16.4": EARININGMEAN2015_TO_2016,
    "2016-17.4": EARININGMEAN2016_TO_2017,
    "2017-18.4": EARININGMEAN2017_TO_2018,
    "2018-19.4": EARININGMEAN2018_TO_2019,
    "2019-20.4": EARININGMEAN2019_TO_2020
}

MALE_POPULATION_COLS_DICT = {
    "S/T name": STATE_NAME,
    "SA2 name": SA2_NAME,
    "SA2 code": SA2_CODE,
    "no.": MALE_PREFIX+AGE0_TO_4,
    "no..1": MALE_PREFIX+AGE5_TO_9,
    "no..2": MALE_PREFIX+AGE10_TO_14,
    "no..3": MALE_PREFIX+AGE15_TO_19,
    "no..4": MALE_PREFIX+AGE20_TO_24,
    "no..5": MALE_PREFIX+AGE25_TO_29,
    "no..6": MALE_PREFIX+AGE30_TO_34,
    "no..7": MALE_PREFIX+AGE35_TO_39,
    "no..8": MALE_PREFIX+AGE40_TO_44,
    "no..9": MALE_PREFIX+AGE45_TO_49,
    "no..10": MALE_PREFIX+AGE50_TO_54,
    "no..11": MALE_PREFIX+AGE55_TO_59,
    "no..12": MALE_PREFIX+AGE60_TO_64,
    "no..13": MALE_PREFIX+AGE65_TO_69,
    "no..14": MALE_PREFIX+AGE70_TO_74,
    "no..15": MALE_PREFIX+AGE75_TO_79,
    "no..16": MALE_PREFIX+AGE80_TO_84,
    "no..17": MALE_PREFIX+AGE85_AND_OVER,
    "no..18": TOTAL_MALES
}




FEMALE_POPULATION_COLS_DICT = {
    "S/T name": STATE_NAME,
    "SA2 name": SA2_NAME,
    "SA2 code": SA2_CODE,
    "no.": FEMALE_PREFIX+AGE0_TO_4,
    "no..1": FEMALE_PREFIX+AGE5_TO_9,
    "no..2": FEMALE_PREFIX+AGE10_TO_14,
    "no..3": FEMALE_PREFIX+AGE15_TO_19,
    "no..4": FEMALE_PREFIX+AGE20_TO_24,
    "no..5": FEMALE_PREFIX+AGE25_TO_29,
    "no..6": FEMALE_PREFIX+AGE30_TO_34,
    "no..7": FEMALE_PREFIX+AGE35_TO_39,
    "no..8": FEMALE_PREFIX+AGE40_TO_44,
    "no..9": FEMALE_PREFIX+AGE45_TO_49,
    "no..10": FEMALE_PREFIX+AGE50_TO_54,
    "no..11": FEMALE_PREFIX+AGE55_TO_59,
    "no..12": FEMALE_PREFIX+AGE60_TO_64,
    "no..13": FEMALE_PREFIX+AGE65_TO_69,
    "no..14": FEMALE_PREFIX+AGE70_TO_74,
    "no..15": FEMALE_PREFIX+AGE75_TO_79,
    "no..16": FEMALE_PREFIX+AGE80_TO_84,
    "no..17": FEMALE_PREFIX+AGE85_AND_OVER,
    "no..18": TOTAL_FEMALES
}

CORRESPONDING_SA2_TO_POSTCODE_COLS_DICT = {
    "POSTCODE": POSTCODE,
    "SA2_MAINCODE_2011":  SA2_CODE,
    "SA2_NAME_2011": SA2_NAME,
    "RATIO": RATIO
}

# BATCH COUNTS AND NAME USED FOR TRAINING VALIDATION AND TESTING
BATCH_NUM = "batch_num"
TRAINING_BATCHES = 10000
NUM_VALIDATIONS = 5

# INDEX FOR BATCHES OF JOINS
NUM_TRANSACTIONS = 600 # Will have to update this constant each time

# CONSUMER_EXTERNAL JOIN DIVIDE BATCHES
CURATED_CONSUMER_EXTERNAL_JOIN_COPY_PATH = CURATED_CONSUMER_EXTERNAL_JOIN_PATH.split(PARQUET)[0] + "_copy" + PARQUET
CURATED_CONSUMER_EXTERNAL_JOIN_PARTITION_FNAME_FORMAT = CURATED_CONSUMER_EXTERNAL_JOIN_PATH.split(PARQUET)[0] + "_{}" + PARQUET
CURATED_CONSUMER_EXTERNAL_JOIN_PARTITION_NUM_BATCHES = 4000

# TRANSACTION TO RAW BATCHES
TRANSACTIONS_RAW_NUM_FILES = 50

# TRANSACTION TO CURATED BATCHES
TRANSACTIONS_TRANSFORM_NUM_FILES = 3
TRANSACTIONS_TRANSFORM_MIN_NUM_FILES = 50

# MERCHANT BATCHES, KEEP ADDDING
MERCHANT_NUM_FILES = 50
MERCHANT_NUM_PER_BATCH = 50
MERCHANT_BATCH_1_IDX = 0
MERCHANT_BATCH_2_IDX = 200
MERCHANT_BATCH_3_IDX = 400

# CONSUMER BATCHES, KEEP ADDING
CONSUMER_NUM_FILES = 1
CONSUMER_NUM_PER_BATCH = 1

# ALL BATCHES, KEEP ADDING
ALL_NUM_FILES = 1
ALL_NUM_PER_BATCH = 1

# OHE BATCHES
OHE_SMALL_NUM_PER_BATCH = 10
OHE_BIG_NUM_PER_BATCH = 1

# OHE COLUMNS
MERCHANT_OHE_COLS = [MERCHANT_ABN, INDUSTRY_TAGS, REVENUE_LEVEL]
CONSUMER_OHE_COLS = [USER_ID, GENDER]
EXTERNAL_OHE_COLS = [SA2_CODE]
TRANSACTIONS_OHE_COLS = [ORDER_DAY_OF_MONTH, ORDER_DAY_OF_WEEK, ORDER_MONTH, ORDER_YEAR]

BIG_CATEGORIES = [MERCHANT_ABN, USER_ID, SA2_CODE]
SMALL_CATEGORIES = [GENDER, INDUSTRY_TAGS, REVENUE_LEVEL, ORDER_MONTH, ORDER_DAY_OF_MONTH, ORDER_DAY_OF_WEEK, ORDER_YEAR]

COL_TO_DISTINCT_VALUES_PATH = {
    INDUSTRY_TAGS : CURATED_INDUSTRY_TAGS_DISTINCTS_PATH,
    REVENUE_LEVEL : CURATED_REVENUE_LEVEL_DISTINCTS_PATH,
    ORDER_MONTH : CURATED_ORDER_MONTH_DISTINCTS_PATH,
    ORDER_DAY_OF_MONTH : CURATED_ORDER_DAY_OF_MONTH_DISTINCTS_PATH,
    ORDER_DAY_OF_WEEK : CURATED_ORDER_DAY_OF_WEEK_DISTINCTS_PATH,
    ORDER_YEAR : CURATED_ORDER_YEAR_DISTINCTS_PATH,
    GENDER: CURATED_GENDER_DISTINCTS_PATH,
    MERCHANT_ABN : CURATED_MERCHANT_ABN_DISTINCTS_PATH,
    SA2_CODE : CURATED_SA2_CODE_DISTINCTS_PATH,
    USER_ID : CURATED_USER_ID_DISTINCTS_PATH
}


FROM_DAY = 1
TO_DAY_OF_WEEK = 7
TO_DAY_OF_MONTH = 31
FROM_MONTH = 1
TO_MONTH = 12
FROM_YEAR = 2021
TO_YEAR = 2022
FROM_REVENUE_LEVEL = 0
TO_REVENUE_LEVEL = 4

COL_TO_DISTINCT_VALUES_LIST = {
    ORDER_DAY_OF_WEEK : [i for i in range(FROM_DAY, TO_DAY_OF_WEEK+1)],
    ORDER_DAY_OF_MONTH : [i for i in range(FROM_DAY, TO_DAY_OF_MONTH+1)],
    ORDER_MONTH : [i for i in range(FROM_MONTH, TO_MONTH+1)],
    ORDER_YEAR : [i for i in range(FROM_YEAR, TO_YEAR+1)],
    GENDER : ["Undisclosed", "Female", "Male"],
    INDUSTRY_TAGS : ['jewelry watch clock silverware', 'shoe', 'computer programming data processing integrated systems design',
                    'stationery office printing writing paper', 'artist supply craft', 'digital goods books movies music',
                    'books periodicals newspapers', 'motor vehicle new parts', 'opticians optical goods eyeglasses',
                    'computers computer peripheral equipment software', 'cable satellite pay television radio',
                    'art dealers galleries', 'music musical instruments pianos sheet music', 'health beauty spas',
                    'hobby toy game', 'gift card novelty souvenir', 'telecom', 'antique repairs restoration',
                    'equipment tool furniture appliance rent al leasing', 
                    'furniture home furnishings equipment manufacturers except appliances',
                    'florists nursery stock flowers', 'watch clock jewelry repair',
                    'lawn garden supply outlets including nurseries',
                    'tent awning','bicycle'],
    # REVENUE_LEVEL : ["a", "b", "c", "d", "e"]
    REVENUE_LEVEL : [i for i in range(FROM_REVENUE_LEVEL, TO_REVENUE_LEVEL+1)]
}

TYPE_CONVERSION_DICT = {
    INDUSTRY_TAGS : str,
    MERCHANT_ABN : int,
    ORDER_DAY_OF_MONTH: int,
    ORDER_YEAR : int,
    ORDER_MONTH: int,
    ORDER_DAY_OF_WEEK: int,
    REVENUE_LEVEL: str,
    USER_ID: int,
    SA2_CODE: int,
    GENDER: str
}

BIG_SAVE_DICT = {
    MERCHANT_ABN : CURATED_MERCHANT_ABN_DICT_PATH,
    USER_ID : CURATED_USER_ID_DICT_PATH,
    SA2_CODE : CURATED_SA2_CODE_DICT_PATH
}

# Values needed for Modelling
TRAIN_BATCH = 5
MAX_FILES = 10
MAX_FILES_RFR = 8
TRAIN_BATCH_RFR = 4
