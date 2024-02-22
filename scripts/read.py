from .constants import *
from .misc_changes import *
from pyspark.sql import DataFrame, SparkSession
import pandas as pd
import geopandas as gpd

# LANDING FILES
def read_landing_consumer_fraud(spark: SparkSession, prefix:str="") -> DataFrame:
    """
    Reads the landing consumer fraud data
    - Parameters
        - spark: SparkSession
        - prefix: prefix used when in different directories
    - Returns
        - Landing Consumer Fraud PySpark Dataframe
    """

    return spark.read.csv(prefix+CONSUMER_FRAUD_PATH, schema=CONSUMER_FRAUD_COLS_SCHEMA,
                          header=True)

def read_landing_merchant_fraud(spark: SparkSession, prefix:str="") -> DataFrame:
    """
    Reads the landing merchant fraud data
    - Parameters
        - spark: SparkSession
        - prefix: prefix used when in different directories
    - Returns
        - Landing Merchant Fraud PySpark Dataframe
    """

    return spark.read.csv(prefix+MERCHANT_FRAUD_PATH, schema=MERCHANT_FRAUD_COLS_SCHEMA,
                          header=True)

def read_landing_consumer_user_details(spark: SparkSession, prefix:str="") -> DataFrame:
    """
    Reads the landing consumer user details data
    - Parameters
        - spark: SparkSession
        - prefix: prefix used when in different directories
    - Returns
        - Landing Consumer User Details PySpark Dataframe
    """

    return spark.read.parquet(prefix+CONSUMER_USER_DETAILS_PATH, schema=CONSUMER_USER_DETAILS_COLS_SCHEMA,
                              header=True)

def read_landing_tbl_consumer(spark: SparkSession, prefix:str="") -> DataFrame:
    """
    Reads the landing table consumer data
    - Parameters
        - spark: SparkSession
        - prefix: prefix used when in different directories
    - Returns
        - Landing Table Consumer PySpark Dataframe
    """

    return spark.read.csv(prefix+TBL_CONSUMER_PATH, schema=TBL_CONSUMER_COLS_SCHEMA,
                          header=True, sep="|")

def read_landing_tbl_merchants(spark: SparkSession, prefix:str="") -> DataFrame:
    """
    Reads the landing table merchant data
    - Parameters
        - spark: SparkSession   
        - prefix: prefix used when in different directories
    - Returns
        - Landing Table Merchant PySpark Dataframe
    """
    return spark.read.parquet(prefix+TBL_MERCHANTS_PATH, schema=TBL_MERCHANTS_COLS_SCHEMA,
                              header=True)

# LANDING TRANSACTION FILES
def read_landing_transaction_files(spark: SparkSession, prefix:str="") -> dict[str, (list[DataFrame], list[str])]:
    """
    Reads the landing transaction data
    - Parameters
        - spark: SparkSession
        - prefix: prefix used when in different directories
    - Returns   
        - Dictionary of transaction names and their respective dataframes
    """
    transactions_dict = {}

    # First get our folders and the folder names
    landing_folders = [f"{prefix}{TABLES_DIR}{fname}" for fname in os.listdir(prefix+TABLES_DIR) if TRANSACTIONS in fname]
    landing_folder_names = [fname for fname in os.listdir(prefix+TABLES_DIR) if TRANSACTIONS in fname]

    # Now loop and get each value
    for i in range(len(landing_folders)):
        transaction_folder = landing_folders[i]
        transaction_name = landing_folder_names[i]
        # Get specific order files and names
        order_names = os.listdir(transaction_folder)
        order_files = [f"{transaction_folder}/{fname}" for fname in os.listdir(transaction_folder)]
        order_files = [spark.read.parquet(fname, schema=TRANSACTIONS_COLS_SCHEMA,
                                        header=True) for fname in order_files if ORDER_DATETIME in fname]
        transactions_dict[transaction_name] = order_files, order_names
    return transactions_dict

# LANDING EXTERNAL FILES
def read_landing_sa2_shape_file(prefix:str="") -> pd.DataFrame:
    """
    Reads landing shape file for SA2 data
    - Parameters
        - prefix: prefix used when accessing files from other directories
    - Returns
        - Pandas SA2 Shape File Dataframe
    """
    return gpd.read_file(prefix+SA2_SHAPE_FILE_PATH)

def read_landing_earning_info(prefix:str="") -> pd.DataFrame:
    """
    Reads landing earning info
    - Parameters
        - prefix: prefix used when accessing files from other directories
    - Returns
        - Pandas Earning Info Dataframe
    """
    return pd.read_excel(prefix+EARNING_INFO_BY_SA2_PATH, sheet_name=4, skiprows=6)

def read_landing_population_info_age_sex(prefix:str="") -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Reads landing population grouped by age per sex
    - Parameters
        - prefix: prefix used when accessing files from other directories
    - Returns
        - A tuple of two dataframes
    """
    male_population_df = pd.read_excel(prefix+POPULATION_INFO_BY_AGE_BY_SEX_BY_SA2_PATH, sheet_name="Table 1", skiprows=7)
    female_population_df = pd.read_excel(prefix+POPULATION_INFO_BY_AGE_BY_SEX_BY_SA2_PATH, sheet_name="Table 2", skiprows=7)
    return male_population_df, female_population_df

def read_landing_sa2_to_postcodes(prefix:str="") -> pd.DataFrame:
    """
    Reads landing SA2 table with corresponding postcodes
    - Parameters
        - prefix: prefix used when accessing files from other directories
    - Returns
        - Pandas SA2 to Postcodes Dataframe
    """
    return pd.read_excel(prefix+CORRESPONDING_SA2_TO_POSTCODE_PATH, sheet_name="Table 3", skiprows=5)

def read_landing_postcodes_to_sa2_2016(prefix: str="") -> pd.DataFrame:
    """
    Reads landing postcodes to SA2 2016 table
    - Parameters
        - prefix: prefix used when accessing files from other directories
    - Returns   
        - Pandas Postcodes to SA2 2016 Dataframe
    """
    return pd.read_csv(prefix+POSTCODE_TO_2016_SA2_PATH)

def read_landing_sa2_2016_to_sa2_2021(prefix: str="") -> pd.DataFrame:
    """
    Reads landing SA2 2016 to SA2 2021 table
    - Parameters
        - prefix: prefix used when accessing files from other directories
    - Returns
        - Pandas SA2 2016 to SA2 2021 Dataframe
    """
    return pd.read_csv(prefix+SA2_2016_TO_SA2_2021_PATH)


# RAW FILES
def read_raw_consumer_fraud(spark: SparkSession, prefix:str="") -> DataFrame:
    """
    Reads the raw consumer fraud data
    - Parameters
        - spark: SparkSession
        - prefix: prefix used when in different directories
    - Returns
        - Raw Consumer Fraud PySpark Dataframe
    """
    return spark.read.csv(prefix+RAW_CONSUMER_FRAUD_PATH, schema=CONSUMER_FRAUD_COLS_SCHEMA,
                          header=True)

def read_raw_merchant_fraud(spark: SparkSession, prefix:str="") -> DataFrame:
    """
    Reads the raw merchant fraud data
    - Parameters
        - spark: SparkSession
        - prefix: prefix used when in different directories
    - Returns
        - Raw Merchant Fraud PySpark Dataframe
    """
    return spark.read.csv(prefix+RAW_MERCHANT_FRAUD_PATH, schema=MERCHANT_FRAUD_COLS_SCHEMA,
                          header=True)

def read_raw_consumer_user_details(spark: SparkSession, prefix:str="") -> DataFrame:
    """
    Reads the raw consumer user details data
    - Parameters
        - spark: SparkSession
        - prefix: prefix used when in different directories
    - Returns
        - Raw Consumer User Details PySpark Dataframe
    """
    return spark.read.parquet(prefix+RAW_CONSUMER_USER_DETAILS_PATH, schema=CONSUMER_USER_DETAILS_COLS_SCHEMA,
                              header=True)

def read_raw_tbl_consumer(spark: SparkSession, prefix:str="") -> DataFrame:
    """
    Reads the raw table consumer data
    - Parameters
        - spark: SparkSession
        - prefix: prefix used when in different directories 
    - Returns
        - Raw Table Consumer PySpark Dataframe
    """
    return spark.read.csv(prefix+RAW_TBL_CONSUMER_PATH, schema=TBL_CONSUMER_COLS_SCHEMA,
                          header=True)

def read_raw_tbl_merchants(spark: SparkSession, prefix:str="") -> DataFrame:
    """
    Reads the raw table merchants data
    - Parameters
        - spark: SparkSession
        - prefix: prefix used when in different directories
    - Returns
        - Raw Table Merchants PySpark Dataframe
    """
    return spark.read.parquet(prefix+RAW_TBL_MERCHANTS_PATH, schema=TBL_MERCHANTS_COLS_SCHEMA,
                              header=True)

# RAW TRANSACTION FILES
def read_raw_transaction_files(spark: SparkSession, prefix:str="") -> dict[str, (list[DataFrame], list[str])]:
    """
    Reads the landing transaction data
    - Parameters
        - spark: SparkSession
        - prefix: prefix used when in different directories
    - Returns
        - A dictionary of transaction dataframes and their corresponding names
    """
    transactions_dict = {}

    # First get our folders and the folder names
    landing_folders = [f"{prefix}{RAW_DIR}{fname}" for fname in os.listdir(prefix+RAW_DIR) if TRANSACTIONS in fname]
    landing_folder_names = [fname for fname in os.listdir(prefix+RAW_DIR) if TRANSACTIONS in fname]

    # Now loop and get each value
    for i in range(len(landing_folders)):
        transaction_folder = landing_folders[i]
        transaction_name = landing_folder_names[i]
        # Get specific order files and names
        order_names = os.listdir(transaction_folder)
        order_files = [f"{transaction_folder}/{fname}" for fname in os.listdir(transaction_folder)]
        order_files = [spark.read.parquet(fname, schema=TRANSACTIONS_COLS_SCHEMA,
                                          header=True) for fname in order_files if ORDER_DATETIME in fname]
        transactions_dict[transaction_name] = order_files, order_names
    return transactions_dict 

# RAW EXTERNAL FILES
# LANDING EXTERNAL FILES
def read_raw_sa2_shape_file(prefix:str="") -> pd.DataFrame:
    """
    Reads raw shape file for SA2 data
    - Parameters    
        - prefix: prefix used when accessing files from other directories   
    - Returns
        - Pandas SA2 shape file dataframe
    """
    return gpd.read_file(prefix+RAW_SA2_SHAPE_FILE_PATH)

def read_raw_earning_info(prefix:str="") -> pd.DataFrame:
    """
    Reads raw earning info
    - Parameters
        - prefix: prefix used when accessing files from other directories
    - Returns
        - Pandas earning info dataframe
    """
    return pd.read_csv(prefix+RAW_EARNING_INFO_BY_SA2_PATH)

def read_raw_population_info_age_sex(prefix:str="") -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Reads raw population grouped by age per sex
    - Parameters
        - prefix: prefix used when accessing files from other directories
    - Returns
        - A tuple of two pandas dataframe
    """
    male_population_df = pd.read_csv(prefix+RAW_MALE_POPULATION_INFO_BY_AGE_BY_SEX_BY_SA2)
    female_population_df = pd.read_csv(prefix+RAW_FEMALE_POPULATION_INFO_BY_AGE_BY_SEX_BY_SA2)
    return male_population_df, female_population_df

def read_raw_sa2_to_postcodes(prefix:str="") -> pd.DataFrame:
    """
    Reads raw SA2 table with corresponding postcodes
    - Parameters
        - prefix: prefix used when accessing files from other directories
    - Returns
        - Pandas dataframe of SA2 to corresponding postcodes
    """
    return pd.read_csv(prefix+RAW_CORRESPONDING_SA2_TO_POSTCODE_PATH)

def read_raw_postcodes_to_sa2_2016(prefix: str="") -> pd.DataFrame:
    """
    Reads raw postcodes to SA2 2016 table
    - Parameters
        - prefix: prefix used when accessing files from other directories
    - Returns
        - Pandas dataframe of postcodes to corresponding SA2 2016
    """
    return pd.read_csv(prefix+RAW_POSTCODE_TO_2016_SA2_PATH)

def read_raw_sa2_2016_to_sa2_2021(prefix: str="") -> pd.DataFrame:
    """
    Reads raw SA2 2016 to SA2 2021 table
    - Parameters
        - prefix: prefix used when accessing files from other directories
    - Returns
        - Pandas dataframe of SA2 2016 to corresponding SA2 2021    
    """
    return pd.read_csv(prefix+RAW_SA2_2016_TO_SA2_2021_PATH)

# CURATED FILES
def read_curated_sa2_shape_file(prefix:str="") -> pd.DataFrame:
    """
    Reads raw shape file for SA2 data
    - Parameters
        - prefix: prefix used when accessing files from other directoriess
    - Returns
        - Pandas SA2 shape file dataframe
    """
    return gpd.read_file(prefix+CURATED_SA2_SHAPE_FILE_PATH)

def read_curated_earning_info(prefix:str="") -> pd.DataFrame:
    """
    Reads raw earning info
    - Parameters
        - prefix: prefix used when accessing files from other directoriess
    - Returns
        - Pandas earning info dataframe
    """
    return pd.read_csv(prefix+CURATED_EARNING_INFO_BY_SA2_PATH)

def read_curated_population_info_age_sex(prefix:str="") -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Reads raw population grouped by age per sex
    - Parameters
        - prefix: prefix used when accessing files from other directoriess
    - Returns
        - A tuple of two pandas dataframe
    """
    male_population_df = pd.read_csv(prefix+CURATED_MALE_POPULATION_INFO_BY_AGE_BY_SEX_BY_SA2)
    female_population_df = pd.read_csv(prefix+CURATED_FEMALE_POPULATION_INFO_BY_AGE_BY_SEX_BY_SA2)
    return male_population_df, female_population_df

def read_curated_sa2_to_postcodes(prefix:str="") -> pd.DataFrame:
    """
    Reads raw SA2 table with corresponding postcodes
    - Parameters
        - prefix: prefix used when accessing files from other directoriess  
    - Returns
        - Pandas dataframe of SA2 to corresponding postcodes
    """
    return pd.read_csv(prefix+CURATED_CORRESPONDING_SA2_TO_POSTCODE_PATH)

# Curated Joined Dataframes
def read_curated_external_join(spark:SparkSession, prefix:str="") -> DataFrame:
    """
    Reads curated joined external dataset
    - Parameters
        - spark: SparkSession to read dataset
        - prefix: prefix used when accessing files from other directoriess
    - Returns
        - PySpark Curated External Join Dataframe
    """
    return spark.read.csv(prefix+CURATED_EXTERNAL_JOIN_PATH, header=True)

def read_curated_tbl_merchant(spark:SparkSession, prefix:str="") -> DataFrame:
    """
    Reads curated table merchant 
    - Parameters
        - spark: SparkSession to read dataset
        - prefix: prefix used when accessing files from other directoriess
    - Returns
        - PySpark Curated External Join Dataframe
    """
    return spark.read.parquet(prefix+CURATED_TBL_MERCHANTS_PATH, header=True)

def read_curated_merchant_fraud(spark:SparkSession, prefix:str="") -> DataFrame:
    return spark.read.csv(prefix+CURATED_MERCHANT_FRAUD_PATH, header=True)

def read_curated_consumer_join(spark:SparkSession, prefix:str="") -> DataFrame:
    """
    Reads curated consumer joined dataset 
    - Parameters
        - spark: SparkSession to read dataset
        - prefix: prefix used when accessing files from other directoriess
    - Returns
        - PySpark Curated External Join Dataframe
    """
    return spark.read.parquet(prefix+CURATED_CONSUMER_JOINED_DATA, header=True)

def read_curated_consumer_external_join(spark:SparkSession, prefix:str="") -> DataFrame:
    """
    Reads curated consumer and external joined dataset 
    - Parameters
        - spark: SparkSession to read dataset
        - prefix: prefix used when accessing files from other directoriess
    - Returns
        - PySpark Curated External Join Dataframe
    """
    return spark.read.parquet(prefix + CURATED_CONSUMER_EXTERNAL_JOIN_PATH, header=True)

# Need this to read a single order file
def read_order_datetime_file(path:str, spark:SparkSession, prefix:str="") -> DataFrame:
    """
    Reads a single order datetime file
    - Parameters
        - path: path to the file
        - spark: SparkSession to read dataset
        - prefix: prefix used when accessing files from other directoriess
    - Returns
        - PySpark Order Datetime Dataframe
    """
    return spark.read.parquet(prefix+path, schema=TRANSACTIONS_COLS_SCHEMA,
                              header=True)
  
def read_curated_tbl_consumer(spark: SparkSession, prefix:str="") -> pd.DataFrame:
    """
    Reads curated consumer data
    - Parameters
        - spark: SparkSession to read dataset
        - prefix: prefix used when accessing files from other directoriess
    - Returns
        - Pandas consumer dataframe
    """

    df = spark.read.csv(prefix+CURATED_TBL_CONSUMER_PATH, schema=TBL_CONSUMER_COLS_SCHEMA, header=True, inferSchema=True)
    pandas_df = df.toPandas()
    return pandas_df

def read_curated_transactions_all(spark: SparkSession, prefix:str="") -> pd.DataFrame:
    """
    Reads curated transactions data
    - Parameters
        - spark: SparkSession to read dataset
        - prefix: prefix used when accessing files from other directoriess
    - Returns
        - Pandas transactions dataframe
    """

    df = spark.read.parquet(prefix+CURATED_TRANSACTIONS_ALL_PATH, header=True)
    return df

def read_curated_external_joined_data(prefix:str="") -> pd.DataFrame:
    """
    Reads curated external data
    - Parameters
        - prefix: prefix used when accessing files from other directoriess
    - Returns
        - Pandas external dataframe
    """
    return pd.read_csv(prefix+CURATED_EXTERNAL_JOINED_DATA)

def read_curated_consumer_user_details_data(spark:SparkSession, prefix:str="") -> DataFrame:
    """
    Reads curated consumer user details data
    - Parameters
        - spark: SparkSession to read dataset
        - prefix: prefix used when accessing files from other directoriess
    - Returns
        - PySpark consumer user details dataframe
    """

    return spark.read.parquet(CURATED_CONSUMER_USER_DETAILS_PATH, header=True)

def read_curated_tbl_consumer_Data(spark:SparkSession, prefix:str="") -> DataFrame:
    """
    Reads curated consumer data
    - Parameters
        - spark: SparkSession to read dataset
        - prefix: prefix used when accessing files from other directoriess
    - Returns
        - PySpark consumer dataframe
    """
    return spark.read.csv(CURATED_TBL_CONSUMER_PATH, header=True)

def read_mapped_industry_data(spark: SparkSession, prefix: str="") -> DataFrame:
    """
    Reads mapped industry data
    - Parameters
        - spark: SparkSession to read dataset
        - prefix: prefix used when accessing files from other directoriess
    - Returns
        - PySpark mapped industry dataframe
    """
    return spark.read.csv(prefix+INDUSTRY_MAPPED, header=True)

# Read files indexing specific order files
def read_index_order_datetime_file(folder:str, index:int,
                              spark:SparkSession, 
                              is_nested:bool=False,
                              prefix:str="") -> tuple[DataFrame, str]:
    """
    Reads a single order datetime file
    - Parameters
        - folder: folder containing the order files
        - index: index of the order file to read
        - spark: SparkSession to read dataset
        - is_nested: whether the folder is nested or not
        - prefix: prefix used when accessing files from other directoriess
    - Returns
        - PySpark Order Datetime Dataframe
    """
    # First get the files from the given folder
    if is_nested:
        transactions_folders = [f"{prefix}{folder}{fname}" for fname in os.listdir(folder)
                                if SNAPSHOT in fname and TRANSACTIONS in fname]
        all_order_files = []
        all_order_names = []
        for folder in transactions_folders:
            order_files = [f"{folder}/{fname}" for fname in os.listdir(folder)
                        if ORDER_DATETIME in fname]
            order_names = [fname for fname in os.listdir(folder)
                        if ORDER_DATETIME in fname]
            all_order_files += order_files
            all_order_names += order_names
        all_order_files = sorted(all_order_files)
        all_order_names = sorted(all_order_names)
    else:
        all_order_files = sorted([f"{prefix}{folder}{fname}" for fname in os.listdir(folder)
                           if ORDER_DATETIME in fname])
        all_order_names = sorted([fname for fname in os.listdir(folder)
                           if ORDER_DATETIME in fname])
    
    # Now get the exact file you want
    indexed_df = read_order_datetime_file(all_order_files[index], spark, prefix)
    indexed_fname = all_order_names[index]
    return indexed_df, indexed_fname

# Need these functions to read the training and test data
def read_train_data(spark:SparkSession, prefix:str="") -> DataFrame:
    """
    Reads in the training data
    - Parameters
        - spark: SparkSession to open the dataframe 
        - prefix: Used for access from different directories
    - Returns
        - Training Data PySpark Dataframe
    """
    return test_train_data(spark, prefix)

def read_train_data_ts(spark:SparkSession, prefix:str="") -> DataFrame:
    """
    Reads in the training data for pandas models (time series)
    - Parameters
        - spark: SparkSession to open the dataframe
        - prefix: Used for access from different directories
    - Returns
        - Training Data PySpark Dataframe
    """
    return test_train_data_ts(spark, prefix)

def read_train_data_neuralnet(spark:SparkSession, prefix:str="") -> DataFrame:
    """
    Reads in the training data for pandas models (time series)
    - Parameters    
        - spark: SparkSession to open the dataframe
        - prefix: Used for access from different directories
    - Returns
        - Training Data PySpark Dataframe
    """
    return test_train_data_neuralnet(spark, prefix)

def read_train_data_rfr(spark:SparkSession, start_ind:int,
                     end_ind:int, prefix:str="") -> DataFrame:
    """
    Test dataframe to test modelling functions
    - Parameters
        - spark: SparkSession to open the dataframe
        - start_ind: start index of the dataframe
        - end_ind: end index of the dataframe
        - prefix: Used for access from different directories
    - Returns
        - Training Data PySpark Dataframe
    """
    return test_train_data_part(spark, start_ind, end_ind, prefix)

def read_test_data(spark:SparkSession, prefix:str="") -> DataFrame:
    """
    Reads in the test data
    - Parameters
        - spark: SparkSession to open the dataframe
        - prefix: Used for access from different directories
    - Returns
        - Test Data PySpark Dataframe
    """
    return test_test_data(spark, prefix)

def read_test_data_rfr(spark:SparkSession, prefix:str="") -> DataFrame:
    """ 
    Reads in the test data of random forest regressor
    - Parameters
        - spark: SparkSession to open the dataframe
        - prefix: Used for access from different directories
    - Returns
        - Test Data PySpark Dataframe
    """
    return test_test_data(spark, prefix)

def read_test_data_neuralnet(spark:SparkSession, prefix:str="") -> DataFrame:
    """
    Reads in the test data of neural network
    - Parameters
        - spark: SparkSession to open the dataframe
        - prefix: Used for access from different directories
    - Returns
        - Test Data PySpark Dataframe
    """

    return test_test_data(spark, prefix)

def read_test_data_ts(spark:SparkSession, prefix:str="") -> DataFrame:
    """
    Reads in the test data of time series models
    - Parameters
        - spark: SparkSession to open the dataframe
        - prefix: Used for access from different directories
    - Returns
        - Test Data PySpark Dataframe
    """
    return test_test_data_ts(spark, prefix)

# Helper Functions to return training and test data
def test_train_data(spark:SparkSession, prefix:str="") -> DataFrame:
    """
    Test dataframe to test modelling functions
    - Parameters
        - spark: SparkSession to open the dataframe
        - prefix: Used for access from different directories
    - Returns
        - Training Data PySpark Dataframe
    """
    transactions_all_files = [f"{prefix}{CURATED_TRANSACTIONS_ALL_PATH}{fname}" \
                              for fname in os.listdir(prefix+CURATED_TRANSACTIONS_ALL_PATH)]
    train_data_files = transactions_all_files[0:9]
    spark = create_spark()
    train_df = spark.read.parquet(*train_data_files)

    drop_columns = get_drop_columns(train_df)
    train_df = train_df.drop(*drop_columns)
    train_df = cast_data_type(train_df)
    return train_df

def test_train_data_part(spark:SparkSession, start_ind:int,
                     end_ind:int, prefix:str="") -> DataFrame:
    """
    Test dataframe to test modelling functions
    - Parameters
        - spark: SparkSession to open the dataframe
        - start_ind: start index of the dataframe
        - end_ind: end index of the dataframe
        - prefix: Used for access from different directories
    - Returns
        - Training Data PySpark Dataframe
    """
    # First 6 files are train, next 3 are test
    transactions_all_files = [f"{prefix}{CURATED_TRANSACTIONS_ALL_PATH}{fname}" \
                              for fname in os.listdir(prefix+CURATED_TRANSACTIONS_ALL_PATH)]
    train_data_files = transactions_all_files[start_ind:end_ind]
    spark = create_spark()
    train_df = spark.read.parquet(*train_data_files)

    # Now save the model
    drop_columns = get_drop_columns(train_df)
    train_df = train_df.drop(*drop_columns)
    train_df = cast_data_type(train_df)
    return train_df

def test_train_data_ts(spark:SparkSession, prefix:str="") -> DataFrame:
    """
    Test dataframe to test modelling functions
    - Parameters
        - spark: SparkSession to open the dataframe
        - prefix: Used for access from different directories
    - Returns
        - Training Data PySpark Dataframe
    """
    transactions_all_files = [f"{prefix}{CURATED_TRANSACTIONS_ALL_PATH}{fname}" \
                              for fname in os.listdir(prefix+CURATED_TRANSACTIONS_ALL_PATH)]
    train_data_files = transactions_all_files[0:6]
    spark = create_spark()
    train_df = spark.read.parquet(*train_data_files)

    drop_columns = get_drop_columns_without_date(train_df)
    train_df = train_df.drop(*drop_columns)
    train_df = cast_data_type(train_df)
    return train_df

def test_train_data_neuralnet(spark:SparkSession, prefix:str="") -> DataFrame:
    """
    Test dataframe to test modelling functions
    - Parameters
        - spark: SparkSession to open the dataframe
        - prefix: Used for access from different directories
    - Returns
        - Training Data PySpark Dataframe
    """
    transactions_all_files = [f"{prefix}{CURATED_TRANSACTIONS_ALL_PATH}{fname}" \
                              for fname in os.listdir(prefix+CURATED_TRANSACTIONS_ALL_PATH)]
    train_data_files = transactions_all_files[0:6]
    spark = create_spark()
    train_df = spark.read.parquet(*train_data_files)

    drop_columns = get_drop_columns(train_df)
    train_df = train_df.drop(*drop_columns)
    train_df = cast_data_type(train_df)
    return train_df

def test_test_data(spark:SparkSession, prefix:str="") -> DataFrame:
    """
    Test in the test data
    - Parameters
        - spark: SparkSession to open the dataframe
        - prefix: Used for access from different directories
    - Returns
        - Test Data PySpark Dataframe
    """
    transactions_all_files = [f"{prefix}{CURATED_TRANSACTIONS_ALL_PATH}{fname}" \
                              for fname in os.listdir(prefix+CURATED_TRANSACTIONS_ALL_PATH)]
    test_data_files = transactions_all_files[9:12]
    spark = create_spark()
    test_df = spark.read.parquet(*test_data_files)

    drop_columns = get_drop_columns(test_df)
    test_df = test_df.drop(*drop_columns)
    test_df = cast_data_type(test_df)
    return test_df

def test_test_data_ts(spark:SparkSession, prefix:str="") -> DataFrame:
    """
    Test the time series test data
    - Parameters
        - spark: SparkSession to open the dataframe
        - prefix: Used for access from different directories
    - Returns
        - Test Data PySpark Dataframe
    """
    transactions_all_files = [f"{prefix}{CURATED_TRANSACTIONS_ALL_PATH}{fname}" \
                              for fname in os.listdir(prefix+CURATED_TRANSACTIONS_ALL_PATH)]
    test_data_files = transactions_all_files[9:12]
    spark = create_spark()
    test_df = spark.read.parquet(*test_data_files)

    drop_columns = get_drop_columns_without_date(test_df)
    test_df = test_df.drop(*drop_columns)
    test_df = cast_data_type(test_df)
    return test_df
    
