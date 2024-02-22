from .constants import *
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql import column as c
from pyspark.ml.feature import StopWordsRemover
import re
import random
from .load import *
from .read import *

BATCH_RANDOM_SEED = 42

def round_dollar_values(sdf: DataFrame) -> DataFrame:
    """
    Round the dollar values of dataframes to 2 decimal places
    - Parameters
        - sdf: PySpark Dataframe
    - Returns
        - Updated PySpark Dataframe with rounded money values
    """

    sdf = sdf.withColumn(DOLLAR_VALUE, F.round(F.col(DOLLAR_VALUE), 2))

    return sdf

def extract_from_tags(sdf: DataFrame) -> DataFrame:
    """
    Extracts from the tags column of a dataframe
    - Parameters
        - sdf: PySpark dataframe
    - Returns
        - Updated PySpark dataframe with extracted columns
    """
    
    # define temporary column constants and regex patterns
    SPLITTED_TAGS = "splitted_tags"
    FILTERED_TAGS = "filtered_tags"
    TAKE_RATE_SUBSTR = "take rate: "
    REPLACEMENT_SUBSTR = ""
    SPLIT_PATTERN = r"[\[\(\)\]]+,*"
    CAPTURE_PATTERN = r"^[^\da-z]*$"
    
    # define the indexes of resulting tags to extract from and float datatype
    INDUSTRY_INDEX = 0
    REV_INDEX = 1
    TAKE_RATE_INDEX = 2
    FLOAT = "float"
    
    # ensure all columns are in lowercase
    sdf = sdf.withColumn(TAGS, F.lower(F.col(TAGS)))
    
    # split the string value for each row in the tags column 
    # into a new column of arrays called splitted tags
    sdf = sdf.withColumn(SPLITTED_TAGS, F.split(TAGS, SPLIT_PATTERN)).drop(TAGS)
    
    # filter the arrays from the splitted tags column to only 
    # include strings with words, letters, and/or numbers
    sdf = sdf.withColumn(FILTERED_TAGS, F.expr(
        "filter(splitted_tags, x -> not rlike(x, '{}'))".format(CAPTURE_PATTERN)
    ))
    
    # form new columns that correspond to each element in the array of strings
    sdf = sdf.withColumns({
        INDUSTRY_TAGS: F.col(FILTERED_TAGS).getItem(INDUSTRY_INDEX),
        REVENUE_LEVEL: F.col(FILTERED_TAGS).getItem(REV_INDEX),
        TAKE_RATE: F.regexp_replace(F.col(FILTERED_TAGS).getItem(TAKE_RATE_INDEX),
                                    TAKE_RATE_SUBSTR, REPLACEMENT_SUBSTR).cast(FLOAT)
    })
    
    # drop the temporary columns
    sdf = sdf.drop(*(SPLITTED_TAGS, FILTERED_TAGS))
    
    # filter the keywords in industry_tags
    sdf = filter_industry_tags(sdf)
    
    # final transformed dataframe
    return sdf

def remove_custom_stopwords(df: DataFrame, in_col: str, out_col: str) -> DataFrame:
    """
    Remove any custom words in the given tags
    - Parameters
        - df     : Input dataframe to be adjusted
        - in_col : Str of column name to remove stop words
        - out_col: Str of output column name after removal
    - Returns
        - Updated Pyspark dataframe with input column removed
    """
    # custom stop words to remove
    CUSTOM_STOPWORDS = ["shops", "service", "services", "sales", "supplies"]
    
    remover = StopWordsRemover(stopWords=CUSTOM_STOPWORDS, inputCol=in_col, outputCol=out_col)
    
    return remover.transform(df).drop(in_col)

def remove_eng_stopwords(df: DataFrame, in_col: str, out_col: str) -> DataFrame:
    """
    Remove any stopwords in the given tags
    - Parameters
        - df     : Input dataframe to be adjusted
        - in_col : Str of column name to remove stop words
        - out_col: Str of output column name after removal
    - Returns
        - Updated Pyspark dataframe with input column removed
    """
    remover = StopWordsRemover(inputCol=in_col, outputCol=out_col)
    
    return remover.transform(df).drop(in_col)

def filter_industry_tags(df: DataFrame) -> DataFrame:
    """
    With the extracted industry tags, filter it to only contain keywords
    - Parameters
        - df: Input dataframe containing industry tags
    - Returns
        - An updated Pyspark dataframe from the input with a filtered industry tag column
    """
    SPLITTED_TAGS = "splitted_tags"
    FILTERED_TAGS = "filtered_tags"
    SPLIT_PATTERN = r"[^\da-zA-Z0-9]*\s+"
    SEP = " "
    
    # split the tags column into words
    df = df.withColumn(SPLITTED_TAGS, F.split(INDUSTRY_TAGS, SPLIT_PATTERN))\
        .drop(INDUSTRY_TAGS)
    # remove any custom stopwords
    df = remove_custom_stopwords(df, SPLITTED_TAGS, FILTERED_TAGS)
    # remove any conventional stopwords
    df = remove_eng_stopwords(df, FILTERED_TAGS, INDUSTRY_TAGS)
    # update the industry tags
    df = df.withColumn(INDUSTRY_TAGS, F.concat_ws(SEP, F.col(INDUSTRY_TAGS)))
 
    return df

def extract_transaction_date_column(transaction_dict: dict[str, (list[DataFrame], list[str])]):
    """
    Extract the dates of transactions from order file names and create a 
    new column in corresponding dataframe.
    - Parameters
        - transaction_dict: Dictionary mapping each transaction folder to its order files and order file names.
    - Returns
        - None, but saves new parquet files wtih the desired new column date
    """
    
    # specify the regex date capture pattern and indexes of relevant datetime
    # and order files
    date_pattern = re.compile(r"\d{4}-\d{2}-\d{2}")
    DATE_FORMAT = "yyyy-MM-dd"
    DATAFRAME_IDX = 0
    DATETIME_IDX = 1
    
    # extract the transaction folders, dataframes and list of dates in separate lists
    transaction_keys = list(transaction_dict.keys())
    transaction_dfs = [val[DATAFRAME_IDX] for _, val in transaction_dict.items()]
    datetime_list = [val[DATETIME_IDX] for _, val in transaction_dict.items()]
    
    # within each folder, find the datetime for each dataframe and 
    # add a new column to correspond with transaction date
    for fol in range(len(transaction_keys)):
        # the number of dataframes in a transaction folder
        num_dfs = len(transaction_dfs[fol])
        for i in range(num_dfs):
            current_transaction = transaction_dfs[fol][i]
            
            # finds the date stamp from each order file name
            date_value = date_pattern.search(datetime_list[fol][i])
            
            # if a valid string is returned, append every row in the new
            # column with the date and convert it to date type
            if date_value:
                transaction_dfs[fol][i] = current_transaction.withColumn(
                    ORDER_DATETIME, F.to_date(F.lit(date_value.group()), DATE_FORMAT)
                )
        # update the transaction dictionary with the amended date columns
        transaction_dict[fol] = tuple((transaction_dfs[fol], datetime_list[fol]))
    
    return transaction_dict

# Functions to do get various values INDIVIDUALLY for each order datetime files
def extract_datetime_order(df: DataFrame, fname:str, prefix:str="") -> DataFrame:
    """
    Extracts the datetime of a single given order_datetime file 
    - Parameters
        - df    : PySpark Order Datetime Dataframe
        - fname : Fname of df where we extract datetime
        - prefix: Prefix for access in different directories
    - Returns
        - DataFrame with a new ORDER_DATETIME column
    """
    # specify the regex date capture pattern and indexes of relevant datetime
    # and order files
    date_pattern = re.compile(r"\d{4}-\d{2}-\d{2}")
    DATE_FORMAT = "yyyy-MM-dd"
    # DATAFRAME_IDX = 0
    # DATETIME_IDX = 1

    # finds the date stamp from each order file name
    date_value = date_pattern.search(fname)
    df = df.withColumn(ORDER_DATETIME, F.to_date(F.lit(date_value.group()), DATE_FORMAT))
    # Now extract the day, month, day of week, and year of the transactions
    df = df.withColumn(ORDER_YEAR, F.year(F.col(ORDER_DATETIME)))
    df = df.withColumn(ORDER_DAY_OF_MONTH, F.dayofmonth(F.col(ORDER_DATETIME)))
    df = df.withColumn(ORDER_DAY_OF_WEEK, F.dayofweek(F.col(ORDER_DATETIME)))
    df = df.withColumn(ORDER_MONTH, F.month(F.col(ORDER_DATETIME)))
    return df

# Use this to encode the revenue level values
def encode_revenue_level(df: DataFrame) -> DataFrame:
    """
    Converts the revenue levels (a, b, c, d, e) into ordinal values
    - Parameters
        - df: PySpark transactions dataframe
    - Returns
        - Updated dataframe with encoded revenue levels
    """
    def str_to_int(x):
        return int(' '.join(format(ord(char), 'b') for char in x), 2)
    binary_to_int = F.udf(str_to_int, IntegerType())
    ASCII_TO_INT = 97
    df = df.withColumn(REVENUE_LEVEL, binary_to_int(F.col(REVENUE_LEVEL)) - ASCII_TO_INT)
    return df

# Use this for to create batch partitinos
def assign_batch_partition(df: DataFrame) -> DataFrame:
    """
    Creates batch partitions for each of the dataframes
    - Parameters
        - df: PySpark dataframe to assign batches to
    - Returns
        - PySpark dataframe with a new column indicating the batch number
    """
    # Define a UDF to generate random numbers
    random.seed(BATCH_RANDOM_SEED)
    def generate_random_number():
        return random.randint(0, TRAINING_BATCHES)
    
    rand_num_udf = F.udf(generate_random_number, IntegerType())

    # Now put this in the dataframe
    df = df.withColumn(BATCH_NUM, rand_num_udf())

    return df

def save_assign_batch_df(folder_idx:int, save_folder:str=CURATED_TRANSACTIONS_ALL_BATCHED_PATH,
                         prefix:str="") -> None:
    """
    Save a given dataframe with the randomly allocated batch numbers as a new column
    - Parameters
        - folder_idx : The specific file to get based on the index
        - save_folder: Folder to save the new dataframe in
        - prefix     : Used for access from different directories
    - Returns
        - None, but saves the newly batched datasets into another folder
    """
    if not os.path.exists(prefix+save_folder):
        os.makedirs(prefix+save_folder)

    spark = create_spark()
    # Get the specific file df and name
    fname = sorted([fname for fname in os.listdir(prefix+CURATED_TRANSACTIONS_ALL_PATH)])[folder_idx]
    fpath = sorted([f"{prefix}{CURATED_TRANSACTIONS_ALL_PATH}{fname}" \
                    for fname in os.listdir(prefix+CURATED_TRANSACTIONS_ALL_PATH)])[folder_idx]
    df = spark.read.parquet(fpath)
    df = assign_batch_partition(df)
    # Save the dataframe
    save_path = f"{save_folder}{fname}"
    load(PARQUET, df, save_path)
    return

# TEST FUNCTIONS
def get_drop_columns(transactions_all:DataFrame) -> list[str]:
    """
    Gets the columns to drop for modelling with the curated transactions dataset
    - Parameters
        - transactions_all: The dataframe indicating the complete and joined curated transactions dataset
    - Returns
        - List of columns to drop for the given dataset
    """
    # Filter out the revenue level columns
    revenue_level_cols = [col for col in transactions_all.columns if REVENUE_LEVEL in col]
    revenue_level_cols = sorted(revenue_level_cols, key=lambda x:len(x))[1:]

    # Drop Male and Female Age coluns
    gender_age_cols = [col for col in transactions_all.columns if "male" in col and GENDER not in col]

    # Order cols
    order_year_cols = [col for col in transactions_all.columns if ORDER_YEAR in col]
    order_year_cols = sorted(order_year_cols, key=lambda x:len(x))[1:]

    order_year_cols = [col for col in transactions_all.columns if ORDER_YEAR in col]
    order_year_cols = sorted(order_year_cols, key=lambda x:len(x))[1:]

    order_year_cols = [col for col in transactions_all.columns if ORDER_YEAR in col]
    order_year_cols = sorted(order_year_cols, key=lambda x:len(x))[1:]

    order_year_cols = [col for col in transactions_all.columns if ORDER_YEAR in col]
    order_year_cols = sorted(order_year_cols, key=lambda x:len(x))[1:]

    order_month_cols = [col for col in transactions_all.columns if ORDER_MONTH in col]
    order_month_cols = sorted(order_month_cols, key=lambda x:len(x))[1:]

    order_dom_cols = [col for col in transactions_all.columns if ORDER_DAY_OF_MONTH in col]
    order_dom_cols = sorted(order_dom_cols, key=lambda x:len(x))[1:]

    order_dow_cols = [col for col in transactions_all.columns if ORDER_DAY_OF_WEEK in col]
    order_dow_cols = sorted(order_dow_cols, key=lambda x:len(x))[1:]

    state_cols = [col for col in transactions_all.columns if STATE in col]
    sa2_cols = [col for col in transactions_all.columns if "sa2" in col]

    # Get all the desired columns
    earningsum_cols = sorted([col for col in transactions_all.columns if "earningsum" in col])[:-1]
    medianage_cols = sorted([col for col in transactions_all.columns if "median_age" in col])[:-1]
    earningmedian_cols = sorted([col for col in transactions_all.columns if "earningmedian" in col])[:-1]
    earningmean_cols = sorted([col for col in transactions_all.columns if "eariningmean" in col])[:-1]
    earners_cols = sorted([col for col in transactions_all.columns if "earners" in col])[:-1]
    industry_tag_cols = sorted([col for col in transactions_all.columns if "industry_tag" in col])
    additional_drop = earningmedian_cols + medianage_cols + earningsum_cols + industry_tag_cols + earningmean_cols + earners_cols + [RATIO]

    # Add the other columns you want to filter out
    categorical_cols = [MERCHANT_ABN, USER_ID, ORDER_ID, ORDER_DATETIME, GENDER, POSTCODE, NAME] + \
        order_dom_cols + order_dow_cols + order_month_cols + order_year_cols + \
        gender_age_cols + revenue_level_cols + state_cols + sa2_cols + \
        additional_drop
    # Drop columns
    return categorical_cols

def get_drop_columns_without_date(transactions_all:DataFrame) -> list[str]:
    """
    Gets the columns to drop for modelling with the curated transactions dataset, but excludes the datetime;
    used for time series models
    - Parameters
        - transactions_all: The dataframe indicating the complete and joined curated transactions dataset
    - Returns
        - List of columns to drop for the given dataset
    """
    # Filter out the revenue level columns
    revenue_level_cols = [col for col in transactions_all.columns if REVENUE_LEVEL in col]
    revenue_level_cols = sorted(revenue_level_cols, key=lambda x:len(x))[1:]

    # Drop Male and Female Age coluns
    gender_age_cols = [col for col in transactions_all.columns if "male" in col and GENDER not in col]

    # Order cols
    order_year_cols = [col for col in transactions_all.columns if ORDER_YEAR in col]
    order_year_cols = sorted(order_year_cols, key=lambda x:len(x))[1:]

    order_year_cols = [col for col in transactions_all.columns if ORDER_YEAR in col]
    order_year_cols = sorted(order_year_cols, key=lambda x:len(x))[1:]

    order_year_cols = [col for col in transactions_all.columns if ORDER_YEAR in col]
    order_year_cols = sorted(order_year_cols, key=lambda x:len(x))[1:]

    order_year_cols = [col for col in transactions_all.columns if ORDER_YEAR in col]
    order_year_cols = sorted(order_year_cols, key=lambda x:len(x))[1:]

    order_month_cols = [col for col in transactions_all.columns if ORDER_MONTH in col]
    order_month_cols = sorted(order_month_cols, key=lambda x:len(x))[1:]

    order_dom_cols = [col for col in transactions_all.columns if ORDER_DAY_OF_MONTH in col]
    order_dom_cols = sorted(order_dom_cols, key=lambda x:len(x))[1:]

    order_dow_cols = [col for col in transactions_all.columns if ORDER_DAY_OF_WEEK in col]
    order_dow_cols = sorted(order_dow_cols, key=lambda x:len(x))[1:]

    state_cols = [col for col in transactions_all.columns if STATE in col]
    sa2_cols = [col for col in transactions_all.columns if "sa2" in col]

    earningsum_cols = sorted([col for col in transactions_all.columns if "earningsum" in col])[:-1]
    medianage_cols = sorted([col for col in transactions_all.columns if "median_age" in col])[:-1]
    earningmedian_cols = sorted([col for col in transactions_all.columns if "earningmedian" in col])[:-1]
    earningmean_cols = sorted([col for col in transactions_all.columns if "eariningmean" in col])[:-1]
    earners_cols = sorted([col for col in transactions_all.columns if "earners" in col])[:-1]
    industry_tag_cols = sorted([col for col in transactions_all.columns if "industry_tag" in col])
    additional_drop = earningmedian_cols + medianage_cols + earningsum_cols + industry_tag_cols + earningmean_cols + earners_cols + [RATIO]


    # Add the other columns you want to filter out
    categorical_cols = [MERCHANT_ABN, USER_ID, ORDER_ID, GENDER, POSTCODE, NAME] + \
        order_dom_cols + order_dow_cols + order_month_cols + order_year_cols + \
        gender_age_cols + revenue_level_cols + state_cols + sa2_cols + \
        additional_drop
    # Drop columns
    return categorical_cols

def cast_data_type(df:DataFrame) -> DataFrame:
    """
    Casts the data type for the transactions all dataset, used specifically for the earning columns
    - Parameters
        - df: PySpark dataframe to cast
    - Returns
        - New dataframe with the type casted columns
    """
    earning_cols = [col for col in df.columns if "earn" in col or "median" in col or "earin" in col]
    for col in earning_cols:
        df = df.withColumn(col, F.col(col).cast("double"))
    # df = df.dropna()
    return df

def drop_columns(df:DataFrame, cols_to_drop:list[str]) -> DataFrame:
    """
    Drops a PySpark dataframe's columns
    - Parameters
        - df          : Input PySpark dataframe
        - cols_to_drop: Columns to drop
    - Returns
        - New dataframe filtered off unnecessary columns
    """
    df = df.drop(*cols_to_drop)
    return df

def transactions_drop_columns(df):
    """
    With the dataframe of transactions, drop any irrelevant columns
    - Parameters:
        - df (DataFrame): Transactions dataframe

    - Returns:
        - additional_drop : Tuple of column names to drop
    """

    # Drop Male and Female Age coluns
    gender_age_cols = [col for col in df.columns if "male" in col]
    gender_cols = [col for col in df.columns if f"{GENDER}_" in col]

    # Drop encoded revenue level columns
    revenue_level_cols = [col for col in df.columns if REVENUE_LEVEL in col]
    revenue_level_cols = sorted(revenue_level_cols, key=lambda x:len(x))[1:]

    # Drop Order_{time} columns for the next 7 chunks of code
    order_year_cols = [col for col in df.columns if ORDER_YEAR in col]
    order_year_cols = sorted(order_year_cols, key=lambda x:len(x))[1:]

    order_year_cols = [col for col in df.columns if ORDER_YEAR in col]
    order_year_cols = sorted(order_year_cols, key=lambda x:len(x))[1:]

    order_year_cols = [col for col in df.columns if ORDER_YEAR in col]
    order_year_cols = sorted(order_year_cols, key=lambda x:len(x))[1:]

    order_year_cols = [col for col in df.columns if ORDER_YEAR in col]
    order_year_cols = sorted(order_year_cols, key=lambda x:len(x))[1:]

    order_month_cols = [col for col in df.columns if ORDER_MONTH in col]
    order_month_cols = sorted(order_month_cols, key=lambda x:len(x))[1:]

    order_dom_cols = [col for col in df.columns if ORDER_DAY_OF_MONTH in col]
    order_dom_cols = sorted(order_dom_cols, key=lambda x:len(x))[1:]

    order_dow_cols = [col for col in df.columns if ORDER_DAY_OF_WEEK in col]
    order_dow_cols = sorted(order_dow_cols, key=lambda x:len(x))[1:]

    # Drop any encoded columns of state and SA2
    state_cols = [col for col in df.columns if f"{STATE_NAME}_" in col]
    sa2_cols = [col for col in df.columns if SA2_NAME in col]

    # Drop columns related to external data like overall earnings and 
    # age data that are not relevant
    earningsum_cols = sorted([col for col in df.columns if "earningsum" in col])[:-1]
    medianage_cols = sorted([col for col in df.columns if "median_age" in col])[:-1]
    earningmedian_cols = sorted([col for col in df.columns if "earningmedian" in col])[:-1]
    earningmean_cols = sorted([col for col in df.columns if "eariningmean" in col])[:-1]
    earners_cols = sorted([col for col in df.columns if "earners" in col])[:-1]
    industry_tag_cols = sorted([col for col in df.columns if "industry_tag" in col])[1:]

    # combine all these columns together
    additional_drop = gender_age_cols + earningmedian_cols + medianage_cols + earningsum_cols +\
        earningmean_cols + earners_cols + [RATIO, SA2_CODE_2016, NAME] + industry_tag_cols +\
            order_dom_cols + order_dow_cols + order_month_cols + order_year_cols + \
                gender_age_cols + revenue_level_cols + gender_cols + state_cols + sa2_cols\
    
    return tuple(additional_drop)

def consumer_external_drop_columns(df):
    """
    With the dataframe of consumer and external data joined,
    drop any irrelevant columns
    - Parameters:
        - df (DataFrame): Consumer and external data joined dataframe

    - Returns:
        - additional_drop : Tuple of column names to drop
    """
    # Drop Male and Female Age coluns
    gender_age_cols = [col for col in df.columns if "male" in col]
    gender_cols = [col for col in df.columns if f"{GENDER}_" in col]

    # Drop encoded revenue level columns
    revenue_level_cols = [col for col in df.columns if REVENUE_LEVEL in col]
    revenue_level_cols = sorted(revenue_level_cols, key=lambda x:len(x))[1:]

    # Drop Order_{time} columns for the next 7 chunks of code
    order_year_cols = [col for col in df.columns if ORDER_YEAR in col]
    order_year_cols = sorted(order_year_cols, key=lambda x:len(x))[1:]

    order_year_cols = [col for col in df.columns if ORDER_YEAR in col]
    order_year_cols = sorted(order_year_cols, key=lambda x:len(x))[1:]

    order_year_cols = [col for col in df.columns if ORDER_YEAR in col]
    order_year_cols = sorted(order_year_cols, key=lambda x:len(x))[1:]

    order_year_cols = [col for col in df.columns if ORDER_YEAR in col]
    order_year_cols = sorted(order_year_cols, key=lambda x:len(x))[1:]

    order_month_cols = [col for col in df.columns if ORDER_MONTH in col]
    order_month_cols = sorted(order_month_cols, key=lambda x:len(x))[1:]

    order_dom_cols = [col for col in df.columns if ORDER_DAY_OF_MONTH in col]
    order_dom_cols = sorted(order_dom_cols, key=lambda x:len(x))[1:]

    order_dow_cols = [col for col in df.columns if ORDER_DAY_OF_WEEK in col]
    order_dow_cols = sorted(order_dow_cols, key=lambda x:len(x))[1:]

    # Drop any encoded columns of state and SA2
    state_cols = [col for col in df.columns if f"{STATE_NAME}_" in col]
    sa2_cols = [col for col in df.columns if SA2_NAME in col]

    # Drop columns related to external data like overall earnings and 
    # age data that are not relevant
    earningsum_cols = sorted([col for col in df.columns if "earningsum" in col])
    medianage_cols = sorted([col for col in df.columns if "median_age" in col])
    earningmedian_cols = sorted([col for col in df.columns if "earningmedian" in col])
    earningmean_cols = sorted([col for col in df.columns if "eariningmean" in col])
    earners_cols = sorted([col for col in df.columns if "earners" in col])
    industry_tag_cols = sorted([col for col in df.columns if "industry_tag" in col])

    # combine all these columns together
    additional_drop = gender_age_cols + earningmedian_cols + medianage_cols + earningsum_cols +\
        earningmean_cols + earners_cols + [RATIO, SA2_CODE_2016, NAME] + industry_tag_cols +\
            order_dom_cols + order_dow_cols + order_month_cols + order_year_cols + \
                gender_age_cols + revenue_level_cols + gender_cols + state_cols + sa2_cols\

    return tuple(additional_drop)
