from .constants import *
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
import sys

def extract(spark: SparkSession, fname:str=None, source:str=None, prefix:str="",) -> DataFrame:
    """
    Creates a dataframe from a given file name or source\
    - Parameters
        - spark: SparkSession object
        - fname : File name if a local dataset
        - source: Website URL if a remote dataset
        - prefix: Prefix for file names depending on directory location
    - Returns
        - PySpark dataframe of a specific file path
    """

    spark = create_spark()

    # For now the opening a local file varies depending on the specific file
    if fname:
        if CONSUMER_FRAUD_PATH in fname:
            output_df = spark.read.csv(prefix+fname, schema=CONSUMER_FRAUD_COLS_SCHEMA, header=True)
        elif CONSUMER_USER_DETAILS_PATH in fname:
            output_df = spark.read.parquet(prefix+fname, schema=CONSUMER_USER_DETAILS_COLS_SCHEMA, header=True)
        elif MERCHANT_FRAUD_PATH in fname:
            output_df = spark.read.csv(prefix+fname, schema=MERCHANT_FRAUD_COLS_SCHEMA, header=True)
        elif TBL_CONSUMER_PATH in fname:
            output_df = spark.read.csv(prefix+fname, schema=TBL_CONSUMER_COLS_SCHEMA, header=True, sep="|")
        elif TBL_MERCHANTS_PATH in fname:
            output_df = spark.read.parquet(prefix+fname, schema=TBL_MERCHANTS_COLS_SCHEMA, header=True)
        elif TRANSACTIONS in fname:
            # For now just read a folder directory
            output_df = spark.read.parquet(prefix+fname+"/*", schema=TRANSACTIONS_COLS_SCHEMA, header=True)
        # Keep this line of code in case we end up needing to read transactions by specific order datetimes
        elif ORDER_DATETIME in fname:
            # Read the specific file within the transactions folder
            output_df = spark.read.parquet(prefix+fname, schema=TRANSACTIONS_COLS_SCHEMA, header=True)
        return output_df
    
    elif source:
        return None

def extract_transactions(spark: SparkSession, prefix:str="") -> dict[str, (list[DataFrame], list[str])]:
    """
    Loads raw/curated files from transactions data
    - Parameters
        - spark : SparkSession to open files
        - prefix: Prefix of the file, used when in different directories
    - Returns
        - Dictionary mapping transaction folders to the respective file dataframes and file names
    """
    
    # Now extract each file in the transactions folder into the path
    transaction_dict ={}

    # Use this to open the order files (exact path needed)
    transaction_folders = [f"{prefix}{TABLES_DIR}{folder}" for folder in os.listdir(prefix+TABLES_DIR) if TRANSACTIONS in folder]
    # Use this to reference the actual names of the eventual folderws in other directories
    transaction_names = [folder for folder in os.listdir(prefix+TABLES_DIR) if TRANSACTIONS in folder]
    for i in range(len(transaction_folders)):
        folder = transaction_folders[i]
        folder_name = transaction_names[i]
        order_names = os.listdir(folder)
        order_files = [f"{folder}/{fname}" for fname in os.listdir(folder)]
        order_files = [extract(spark, fname) for fname in order_files if ORDER_DATETIME in fname]
        transaction_dict[folder_name] = order_files, order_names

    return transaction_dict

def extract_order_file(spark: SparkSession, index:int, prefix:str="") -> tuple[DataFrame, str]:
    """
    Alternative extract transactions function that only acts on a single order datetime file
    - Parameters
        - spark : SparkSession to open files
        - index : The index of the order_datetime file based
        - prefix: Prefix of the file, used when in different directories
    - Returns
        - Dataframe of a single order datetime file in a transactions folder and the file name
    """
    # First get the total list of transactions
    transactions_folders = [f"{prefix}{TABLES_DIR}{fname}" for fname in os.listdir(TABLES_DIR)
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
    # Now return the dataframe and the file name
    return extract(spark, all_order_files[index]), all_order_names[index]