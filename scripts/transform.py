# Extraction functions taken from:
# https://blog.devgenius.io/basic-etl-using-pyspark-ed08b7e53cf4


from pyspark.sql import DataFrame
import pandas as pd


def rename_cols(df: DataFrame, mapping_dict: dict) -> DataFrame:
    """
    Renames the columns of a given PySpark dataframe
    - Parameters
        - df: Input dataframe
        - mapping_dict: Dictionary of column names to rename
    - Returns
        - Dataframe with renamed columns
    """
    for key in mapping_dict.keys():
        df=df.withColumnRenamed(key,mapping_dict.get(key))
    return df

def rename_transactions(transaction_dict: dict[str, (list[DataFrame], list[str])], mapping_dict: dict) -> \
    dict[str, (list[DataFrame], list[str])]:
    """
    Rename the columns in the transactions folders
    - Parameters
        - transaction_dict: Dictionary of transaction folders
        - mapping_dict: Dictionary of column names to rename
    - Returns
        - Dataframe with renamed columns
    """
    for transaction_folder in transaction_dict:
        new_files = []
        order_files, order_names = transaction_dict[transaction_folder]
        for order_df in order_files:
            for key in mapping_dict.keys():
                order_df=order_df.withColumnRenamed(key,mapping_dict.get(key))
            new_files.append(order_df)
        transaction_dict[transaction_folder] = new_files, order_names
    return transaction_dict


def specific_cols(df: DataFrame, specific_cols: list):
    """
    Get specific columns from a dataframe
    - Parameters
        - df: Input dataframe
        - specific_cols: List of columns to get
    - Returns
        - Dataframe with specific columns
    """
    # get specific cols df
    return df.select(specific_cols)



def join_df(left_df: DataFrame, right_df: DataFrame, on_columns:list,  join_type: str)->DataFrame:
    # Join two dataframes
    '''
    Thhis function joins two dataframes
    - Parameters
        - left_df: Input dataframe
        - right_df: Input dataframe
        - on_columns: List of columns to perform join
        - join_type: Join type
    - Returns
        - Dataframe with joined columns
    '''
    output_df=left_df.alias("left_df").join(right_df.alias("right_df"), on_columns, join_type)
    return output_df

def join_df_pd(left_df: pd.DataFrame, right_df: pd.DataFrame, on_columns:list, join_type: str)->DataFrame:
    """
    Join pandas dataframes
    - Parameters
        - left_df: Input dataframe
        - right_df: Input dataframe 
        - on_columns: List of columns to perform join
        - join_type: Join type
    - Returns
        - Dataframe with joined columns
    """
    return left_df.merge(right_df,
                            how=join_type,
                            on=on_columns)