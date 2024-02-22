# Load functions taken from:
# https://blog.devgenius.io/basic-etl-using-pyspark-ed08b7e53cf4

from pyspark.sql import DataFrame
from .constants import *


def load(type: str, df: DataFrame, target: str, prefix:str="") -> None:
    """
    Function is primarily used to save data files in either the raw or curated data folders
    - Parameters
        - type: Input Storage type
        - df: Input Dataframe
        - target: Input file path for storage
    - Returns
        - None
    """

    HEADER = "header"
    TRUE = "true"
    # Load the data based on type; write data on mysql database with table name
    if type==CSV:
        df.write.mode(OVERWRITE).option(HEADER, TRUE).csv(prefix+target)
    
    if type==PARQUET:
        df.write.mode(OVERWRITE).parquet(prefix+target)

def load_transactions(path:str, transaction_dict: dict[str, (list[DataFrame], list[str])], prefix:str="") -> None:
    """
    Loads raw/curated files from transactions data
    - Parameters
        - path: Path to the data folder
        - transaction_dict: Dictionary of transaction data
        - prefix: Prefix used for access in other directories
    - Returns
        - None
    """

    # For each key and associated value, need to save every file
    # in a new transaction folder in a specifc data folder (raw, curated)
    for folder_name, (order_files, order_names) in transaction_dict.items():
        # First make the directory
        transaction_folder_path = f"{prefix}{path}{folder_name}"
        if not os.path.exists(transaction_folder_path):
            os.makedirs(transaction_folder_path)
        
        # Then save every df in that folder to the save path
        for i in range(len(order_files)):
            order_name = order_names[i]
            order_file = order_files[i]
            save_path = f"{transaction_folder_path}/{order_name}"
            order_file.write.mode(OVERWRITE).parquet(save_path)

# Load COPIES of files (save)
def save_copy_df(file_type:str, open_path:DataFrame, prefix:str="") -> None:
    """
    Create a saved copy of a given dataframe (assumes the schema can be inferred)
    - Parameters
        - file_type: Type of file to save
        - open_path: Path to the file to open
        - prefix: Prefix used for access in other directories
    - Returns
        - None
    """

    INVALID_FILE_TYPE_MSG = "INVALID FILE TYPE"
    spark = create_spark()
    
    if file_type == PARQUET:
        df = spark.read.parquet(prefix+open_path)
    elif file_type == CSV:
        df = spark.read.csv(prefix+open_path)
    else:
        raise ValueError(INVALID_FILE_TYPE_MSG)
    
    # Now save the dataframe into a new path
    separate_file_suffix = open_path.split(file_type)[0]
    new_fpath = f"{prefix}{separate_file_suffix}_copy{file_type}" 
    # print(new_fname)
    # print(CURATED_CONSUMER_EXTERNAL_JOIN_COPY_PATH)
    load(file_type, df, new_fpath, prefix)