from .constants import *
from pyspark.sql import DataFrame, functions as F
from pyspark.ml.feature import OneHotEncoder
from pyspark.ml.functions import vector_to_array
from itertools import chain
from functools import reduce
from .read import *
from .load import *
import json

def ohe_specific_col(sdf:DataFrame, curr_col:str, distinct_vals:list, reduce_cols:bool=False) -> tuple[DataFrame, dict]:
    """
    One hot encodes all the relevant columns of the final joined dataframe
    - Parameters
        - sdf: Input DataFrame
        - curr_col: Current column to one hot encode
        - distinct_vals: List of distinct values in the column
        - reduce_cols: Boolean to indicate whether to reduce the columns
    - Returns
        - sdf: DataFrame with one hot encoded columns
    """
    print("DOING: "+curr_col)
    # First get the unique values
    print("MAKING UNIQUE VALUES")
    # Then map each to an integer
    print("MAPPING INTS TO CATEGORIES")
    convert_dict = {}
    i = 0
    for val in distinct_vals:
        convert_dict[val] = i
        i += 1
    # Afterwards, substitute the values
    mapping_expr = F.create_map([F.lit(x) for x in chain(*convert_dict.items())])

    sdf = sdf.withColumn(curr_col+"_idx", mapping_expr[F.col(curr_col)])

    # Now, one hot encode
    print("MAKING ONE HOT ENCODED COLUMN")
    ohe = OneHotEncoder(inputCols=[curr_col+"_idx"],
                outputCols=[curr_col+"_ohe"])
    model = ohe.fit(sdf)
    sdf= model.transform(sdf)

    print("MAKING NEW COLUMNS (JUST THE VECTOR)")
    sdf = sdf.drop(curr_col+"idx")
    print("FINNISHED OHE")

    return sdf, convert_dict

def ohe_cols(sdf:DataFrame) -> DataFrame:
    """
    One hot encodes all the relevant columns of the final joined dataframe
    - Parameters
        - sdf: Fianl joined DataFrame
    - Returns
        - sdf: DataFrame with one hot encoded columns
    """
    ohe_cols = [col for col in sdf.columns if col in SMALL_CATEGORIES + BIG_CATEGORIES]
    print("STARTING OHE")
    for curr_col in ohe_cols:
        print("DOING: "+curr_col)
        # First get the unique values
        print("MAKING UNIQUE VALUES")
        unique_vals = [row[curr_col] for row in sdf.select(F.col(curr_col)).distinct().collect()]
        # Then map each to an integer
        print("MAPPING INTS TO CATEGORIES")
        convert_dict = {}
        i = 0
        for val in unique_vals:
            convert_dict[val] = i
            i += 1
        # Afterwards, substitute the values
        mapping_expr = F.create_map([F.lit(x) for x in chain(*convert_dict.items())])

        sdf = sdf.withColumn(curr_col+"_idx", mapping_expr[F.col(curr_col)])

        # Now, one hot encode
        print("MAKING ONE HOT ENCODED COLUMN")
        ohe = OneHotEncoder(inputCols=[curr_col+"_idx"],
                    outputCols=[curr_col+"_ohe"])
        model = ohe.fit(sdf)
        sdf= model.transform(sdf)

        # After one hot encoding, need to make the new one hot encoded columns
        print("MAKING NEW COLUMNS")
        sdf = sdf \
                    .withColumn(curr_col+"_val", vector_to_array(curr_col+"_ohe")) \
                    .select(sdf.columns + [F.col(curr_col+"_val")[i] for i in range(len(convert_dict.keys()))]) \
                    .drop(curr_col+"_ohe").drop(curr_col+"_idx")
    print("FINNISHED OHE")
    return sdf

def ohe_reduce_cols(sdf:DataFrame, curr_col:str, prefix:str="") -> DataFrame:
    """
    One hot encodes all the relevant columns of the final joined dataframe
    - Parameters
        - sdf: Fianl joined DataFrame
    - Returns
        - sdf: DataFrame with one hot encoded columns
    """
    distinct_values = COL_TO_DISTINCT_VALUES_LIST[curr_col]


    # print(f"CURRENT COLUMN : {curr_col}")
    sdf = reduce(
        lambda df, category: df.withColumn(f'{curr_col}_{category}', \
            F.when(F.col(curr_col) == category, F.lit(1)).otherwise(0)),
        distinct_values,
        sdf
    )
    return sdf

def ohe_small_cols(index:int,
                   file_type:str=PARQUET,
                   open_folder:str=CURATED_TRANSACTIONS_ALL_PATH,
                   save_folder:str=CURATED_TRANSACTIONS_OHE_PATH,
                   prefix:str="") -> None:
    """
    One hot encodes all the small categorical columns
    - Parameters
        - index: Index of the file to one hot encode
        - file_type: File type of the file to one hot encode
        - open_folder: Folder to open the file from
        - save_folder: Folder to save the file to
        - prefix: Prefix of the file to one hot encode
    - Returns
        - None
    """
    # First read in the dataframe
    spark = create_spark()
    sdf, curr_fname = read_index_order_datetime_file(prefix+open_folder, index, spark)

    # Now one hot encode the relevant columns
    # print("ONE HOT ENCODING SMALL CATEGORICAL FEATURES...")
    for col in SMALL_CATEGORIES:
        sdf = ohe_reduce_cols(sdf, col)
    # print("FINISHED ONE HOT ENCODING SMALL CATEGORICAL FEATURES")
    
    # Now save the dataframe
    # print("SAVING DATAFRAME...")
    save_path = save_folder+curr_fname
    load(file_type, sdf, save_path, prefix)
    # print("FINISEHD SAVING DATAFRAME")
    return

def ohe_big_col(index:int,
               file_type:str=PARQUET,
               open_folder:str=CURATED_TRANSACTIONS_ALL_PATH,
               save_folder:str=CURATED_TRANSACTIONS_OHE_PATH,
               prefix:str="") -> None:
    """
    One hot encodes all the big categorical columns
    - Parameters
        - index: Index of the file to one hot encode
        - file_type: File type of the file to one hot encode
        - open_folder: Folder to open the file from
        - save_folder: Folder to save the file to
        - prefix: Prefix of the file to one hot encode
    - Returns
        - None
    """
    # First read in the dataframe
    spark = create_spark()
    sdf, curr_fname = read_index_order_datetime_file(prefix+open_folder, index, spark)

    # Get out distinct values
    for curr_col in BIG_CATEGORIES:
        print(f"OHE OF COL: {curr_col}")
        print("GETTING DISTINCT VALUES...")
        distinct_values = None
        distinct_values_path = COL_TO_DISTINCT_VALUES_PATH[curr_col]
        with open(distinct_values_path, "r") as fp:
            distincts_words = fp.read()[:-1].split("\n")
            # print(distincts_words[-10:])
            distincts_words = [(TYPE_CONVERSION_DICT[curr_col])(val) for val in distincts_words]
            distinct_values = distincts_words
        print("FINISHED GETTING DISTINCT VALUES")

        # Now one hot encode the relevant column
        print("DOING OHE FOR "+curr_col+"...")
        sdf, convert_dict = ohe_specific_col(sdf, curr_col, distinct_values)
        print("FINISHED DOING OHE FOR "+curr_col)

        # Save the conversion dictionary into curated if it doesn't exist
        dict_save_path = BIG_SAVE_DICT[curr_col]
        if not os.path.exists(prefix+dict_save_path):
            print(f"SAVING CONVERSION DICTIONARY OF {curr_col}")
            with open(dict_save_path, "w") as fp:
                json.dump(convert_dict, fp)

    # Now save the path
    print("SAVING DATAFRAME...")
    save_path = prefix+save_folder+curr_fname
    load(file_type, sdf, save_path, prefix)
    print("FINISHED SAVING DATAFRAME")
    return

# FINAL FUNCTIONS

def ohe_from_txt(sdf: DataFrame, curr_col:str,
                 reduce_df:bool=False,
                 prefix:str="") -> None:
    """
    One hot encodes based on a given list of values from
    - Parameters
        - sdf: DataFrame to one hot encode
        - curr_col: Column to one hot encode    
        - reduce_df: Whether to reduce the dataframe or not
        - prefix: Prefix of the file to one hot encode
    - Returns
        - None
    """

    # First extract the needed list of entries
    distincts_list = []
    distincts_path = COL_TO_DISTINCT_VALUES_PATH[curr_col]
    with open(prefix+distincts_path, "r") as fp:
        # print(fp.read()[-1])
        distincts_words = fp.read()[:-1].split("\n")
        print(distincts_words[-10:])
        distincts_words = [(TYPE_CONVERSION_DICT[curr_col])(val) for val in distincts_words]
        distincts_list = distincts_words
    
    # Now one hot encode
    # sdf = ohe_col(sdf, curr_col).drop(curr_col)
    sdf = ohe_specific_col(sdf, curr_col, distincts_list, reduce_df)

    return sdf

def ohe_small_cols_batch(start_idx:int,
                        end_idx:int,
                        file_type:str=PARQUET,
                        open_folder:str=CURATED_TRANSACTIONS_ALL_PATH,
                        save_folder:str=CURATED_TRANSACTIONS_OHE_PATH,
                        prefix:str="") -> None:
    """
    One hot encodes all the small categorical columns
    - Parameters
        - start_idx: Start index of the batch
        - end_idx: End index of the batch
        - file_type: File type of the file to one hot encode
        - open_folder: Folder to open the file from
        - save_folder: Folder to save the file to
        - prefix: Prefix of the file to one hot encode
    - Returns
        - None
    """
    print(f"DOING BATCH FROM {start_idx} to {end_idx}")
    curr_idx = start_idx
    while curr_idx < end_idx:
        print(f"CURRENT IDX: {curr_idx}")
        ohe_small_cols(curr_idx,
                       file_type,
                       open_folder,
                       save_folder,
                       prefix)
        curr_idx += 1
    return

def ohe_big_cols_batch(start_idx:int,
                       end_idx:int,
                    #    curr_col:str,
                       file_type:str=PARQUET,
                       open_folder:str=CURATED_TRANSACTIONS_OHE_PATH,
                       save_folder:str=CURATED_TRANSACTIONS_OHE_BIG_PATH,
                       prefix:str="") -> None:
    """
    One hot encodes all the big categorical columns
    - Parameters
        - start_idx: Start index of the batch
        - end_idx: End index of the batch
        - file_type: File type of the file to one hot encode
        - open_folder: Folder to open the file from
        - save_folder: Folder to save the file to
        - prefix: Prefix of the file to one hot encode
    - Returns
        - None
    """
    print(f"DOING FROM INDEX {start_idx} to {end_idx}")
    curr_idx = start_idx

    # Make a new directory for the big OHE columns


    while curr_idx < end_idx:
        print(f"CURRENT IDX: {curr_idx}")
        ohe_big_col(curr_idx,
                    file_type,
                    open_folder,
                    save_folder,
                    prefix)
        curr_idx += 1
    return