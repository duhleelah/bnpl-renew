from .constants import *
from .transform import *
from .read import *
from .load import *
import time

# JOIN ORDER
# Consumer Details Inner Join Mercchant Details Inner Join Transactions ON Merchant ABN and Customer ID
#   - Do it per order

# INTERNAL DATASETS
def join_consumer_data(tbl_consumer: DataFrame, user_details: DataFrame, consumer_fraud: DataFrame):
    """
    Join the consumer-related dataframes into one dataframe
    - Parameters
        - tbl_consumer: Input dataframe for tbl consumer data
        - user_details: Input dataframe for mapping table of IDs
        - consumer_fraud: Input dataframe for consumer fraud probability
    - Returns
        - extended_consumer_details: Joined dataframe of consumer data
    """
    
    # firstly, join the consumer personal details and the user mapping table
    mapped_ids = join_df(tbl_consumer, user_details, [CONSUMER_ID], INNER_JOIN)
    
    # after mapping the consumer IDs to the user IDs, 
    # map the user ID to its fraud probability
    extended_consumer_details = join_df(mapped_ids, consumer_fraud, [USER_ID], OUTER_JOIN)
    
    return extended_consumer_details

def join_consumer_tbl_details(tbl_consumer: DataFrame, user_details: DataFrame) -> DataFrame:
    """
    Join consumer user details with consumer table
    - Parameters
        - tbl_consumer: Input dataframe for tbl consumer data
        - user_details: Input dataframe for mapping table of IDs
    - Returns
        - consumer_details: Joined dataframe of consumer data
    """

    return join_df(tbl_consumer, user_details, [CONSUMER_ID], INNER_JOIN)

def join_merchant_data(tbl_merchants: DataFrame, merchant_fraud:DataFrame):
    """
    Join the consumer-related dataframes into one dataframe
    - Parameters
        - tbl_merchants: Input dataframe for tbl merchant data
        - merchant_fraud: Input dataframe for merchant fraud probability
    - Returns
        - merchant_details: Joined dataframe of merchant data
    """
    
    # join the two merchant dataframes on their ABNs
    # merchant_details = join_df(tbl_merchants, merchant_fraud, [MERCHANT_ABN], LEFT_JOIN)
    merchant_details = join_df(tbl_merchants, merchant_fraud, [MERCHANT_ABN], OUTER_JOIN)
    
    return merchant_details

def join_consumer_external(consumer_joined:DataFrame, external_data:DataFrame) -> DataFrame:
    """
    Merge the consume and external dataset
    - Parameters
        - consumer_joined: Consumer data joined with merchant data
        - external_data: External data to join with
    - Returns
        - consumer_external: Joined dataframe of consumer and external data
    """

    # If ever drop unnecessary rows here
    return join_df(consumer_joined, external_data, [STATE, POSTCODE], INNER_JOIN)

# EXTERNAL DATA
def join_external_data(sa2_shape: pd.DataFrame, sa2_postcode: pd.DataFrame, population_male: pd.DataFrame, population_female:pd.DataFrame, earnings_sa2: pd.DataFrame) -> pd.DataFrame:
    """
    Add the desired states from the shape file into the sa2_postcode
    - Parameters
        - sa2_shape: Input dataframe for sa2 shape file
        - sa2_postcode: Input dataframe for sa2 postcode
        - population_male: Male population dataframe
        - population_female: Female population dataframe
        - earnings_sa2: Earnings dataframe
    - Returns
        - total_external: Joined dataframe of external data
    """

    AXIS = 1

    # Cast datatypes of the join key first
    sa2_postcode[[SA2_CODE_2016, SA2_CODE_2021]] = sa2_postcode[[SA2_CODE_2016, SA2_CODE_2021]].astype(str)
    population_female[SA2_CODE] = population_female[SA2_CODE].astype(str)
    population_male[SA2_CODE] = population_male[SA2_CODE].astype(str)
    earnings_sa2[SA2_CODE] = earnings_sa2[SA2_CODE].astype(str)

    # Now do the merges
    total_external = sa2_postcode.merge(sa2_shape,
                                        left_on=SA2_CODE_2021,
                                        right_on=SA2_CODE,
                                        how = INNER_JOIN)
    total_external = total_external.merge(earnings_sa2,
                                          left_on=SA2_CODE_2016,
                                          right_on=SA2_CODE,
                                          how= INNER_JOIN
                                          ).rename({
                                              SA2_CODE_2021:SA2_CODE
                                          }, axis=AXIS)
    # total_external = total_external.drop([SA2_CODE_X, SA2_CODE_Y], axis=1)
    total_external = total_external.merge(population_male,
                                          on=SA2_CODE,
                                          how = INNER_JOIN
                                          ).drop([SA2_CODE_X, SA2_CODE_Y, SA2_NAME_X, SA2_NAME_Y], axis=1)
    total_external = total_external.merge(population_female,
                                          on=SA2_CODE,
                                          how = INNER_JOIN
                                          )
    # .drop([SA2_CODE_X, SA2_CODE_Y], axis=1)

    # Now remap the state values to actual values
    total_external.replace(STATE_CONVERT, inplace=True)

    # Now after merging, you want to drop the irrelevant columns
    total_external.drop([GEOMETRY, SA2_NAME_X, SA2_NAME_Y, STATE_NAME_X, STATE_NAME_Y], axis=1, inplace=True)

    return total_external

def join_consumer_and_external_data(consumer_data: DataFrame, external_data: DataFrame):
    """
    Join the consumer details with the external data
    - Parameters
        - consumer_data: Input dataframe for consumer data
        - external_data: Input dataframe for external data
    - Returns
        - joined_df: Joined dataframe of consumer and external data
    """
    
    joined_df = consumer_data.merge(external_data, on=POSTCODE, suffixes=("_consumer", "_external"), how=INNER_JOIN)
    return joined_df

def join_consumer_and_external_male_and_female_ratio(consumer_data: DataFrame, external_data: DataFrame):
    """
    Join consumer data and external data for the ratio of male and female
    - Parameters
        - consumer_data: Input dataframe for consumer data
        - external_data: Input dataframe for external data
    - Returns
        - joined_df: Joined dataframe of consumer and external data
    """

    merged_df = consumer_data.merge(external_data, left_on=STE_NAME, right_on=STATE, how=INNER_JOIN)
    merged_df.drop(columns=[STATE], inplace=True)
    return merged_df

# Big Join Functions for Transaction Data

# Helper function to join and save exactly one dataframe
def join_merchant_order_datetime(spark:SparkSession, order_fpath:str, save_path:str, prefix:str="") -> None:
    """
    Join the order and datetime dataframes with the merchant dataframes
    - Parameters
        - spark: Spark session
        - order_fpath: Order file path
        - save_path: Save path
        - prefix: Prefix for the save file
    - Returns
        - None
    """

    # # First make necessary dataframes
    REPARTITION_VALUE = 200
    order_file = read_order_datetime_file(order_fpath, spark)
    merchant_details = read_curated_tbl_merchant(spark, prefix)

    # Now join the dataframes
    order_file = join_df(order_file, merchant_details, [MERCHANT_ABN], INNER_JOIN)
    load(PARQUET, order_file, save_path)
    return

def join_consumer_order_datetime(spark:SparkSession, order_fpath:str, save_path:str, prefix:str="") -> None:
    """
    Join the order and datetime dataframes with the consumer dataframes
    - Parameters
        - spark: Spark session
        - order_fpath: Order file path
        - save_path: Save path
        - prefix: Prefix for the save file
    - Returns
        - None
    """

    # First make necessary dataframes
    REPARTITION_VALUE = 200
    order_file = read_order_datetime_file(order_fpath, spark)#.repartition(REPARTITION_VALUE)
    consumer_details = read_curated_consumer_external_join(spark, prefix).repartition(REPARTITION_VALUE)
    # consumer_details = read_curated_consumer_join(spark, prefix)

    # Let's rename the columns
    order_file = order_file.withColumnRenamed(NAME, MERCHANT_PREFIX+NAME)
    # consumer_details = consumer_details.drop(NAME, ADDRESS, CONSUMER_ID).withColumnRenamed(NAME, CONSUMER_PREFIX+NAME)
    # consumer_details = consumer_details.drop(NAME, ADDRESS, CONSUMER_ID)

    # Now join the dataframes
    order_file = join_df(order_file, consumer_details, [USER_ID], INNER_JOIN)
    # print(f"CURRENT PATH : {save_path}")
    load(PARQUET, order_file, save_path)
    return


def join_merchant_transactions(spark: SparkSession, open_folder:str, save_folder:str, start_idx:int, num_files:int=50, prefix:str="") -> None:
    """
    Join the merchant transactions with the merchant dataframes
    - Parameters
        - spark: Spark session
        - open_folder: Folder to open
        - save_folder: Folder to save
        - start_idx: Start index
        - num_files: Number of files to join
        - prefix: Prefix for the save file
    - Returns
        - None
    """

    # First we extract each of the files
    order_files_list = []
    order_names_list = []
    if open_folder != CURATED_DIR:
        order_names_list = [fname for fname in os.listdir(prefix+open_folder) if ORDER_DATETIME in fname]
        order_files_list = [f"{prefix}{open_folder}/{fname}" for fname in os.listdir(prefix+open_folder) if ORDER_DATETIME in fname]
    else:
        transaction_folders = [f"{prefix}{open_folder}{fname}" for fname in os.listdir(prefix+open_folder) \
                        if TRANSACTIONS in fname and SNAPSHOT in fname]
        for folder in transaction_folders:
            order_names = [fname for fname in os.listdir(folder) if ORDER_DATETIME in fname]
            order_files = [f"{folder}/{fname}" for fname in os.listdir(folder) if ORDER_DATETIME in fname]
            order_files_list += order_files
            order_names_list += order_names
    order_files_list = sorted(order_files_list)
    order_names_list = sorted(order_names_list)

    # Now we only want to get a number of files from a certain index
    order_files = order_files_list[start_idx:(start_idx+num_files)]
    order_names = order_names_list[start_idx:(start_idx+num_files)]

    # Now iterate and do desired operations
    for i in range(len(order_files)):
        order_fpath = order_files[i]
        order_fname = order_names[i]
        save_fpath = f"{prefix}{save_folder}{order_fname}"
        join_merchant_order_datetime(spark, order_fpath, save_fpath, prefix)
    return

def join_consumer_transactions(spark: SparkSession, open_folder:str, save_folder:str, start_idx:int, num_files:int=50, prefix:str="") -> None:
    """
    Join the consumer transactions with the consumer dataframes
    - Parameters
        - spark: Spark session
        - open_folder: Folder to open
        - save_folder: Folder to save
        - start_idx: Start index
        - num_files: Number of files to join
        - prefix: Prefix for the save file
    - Returns
        - None
    """

    # First we extract each of the files
    order_files_list = []
    order_names_list = []
    if open_folder != CURATED_DIR:
        order_names_list = [fname for fname in os.listdir(prefix+open_folder) if ORDER_DATETIME in fname]
        order_files_list = [f"{prefix}{open_folder}/{fname}" for fname in os.listdir(prefix+open_folder) if ORDER_DATETIME in fname]
    else:
        transaction_folders = [f"{prefix}{open_folder}{fname}" for fname in os.listdir(prefix+open_folder) \
                        if TRANSACTIONS+"_" in fname]
        for folder in transaction_folders:
            order_names = [fname for fname in os.listdir(folder) if ORDER_DATETIME in fname]
            order_files = [f"{folder}/{fname}" for fname in os.listdir(folder) if ORDER_DATETIME in fname]
            order_files_list += order_files
            order_names_list += order_names
    order_files_list = sorted(order_files_list)
    order_names_list = sorted(order_names_list)

    # Now we only want to get a number of files from a certain index
    order_files = order_files_list[start_idx:(start_idx+num_files)]
    order_names = order_names_list[start_idx:(start_idx+num_files)]

    # Now iterate and do desired operations
    for i in range(len(order_files)):
        order_fpath = order_files[i]
        order_fname = order_names[i]
        save_fpath = f"{prefix}{save_folder}{order_fname}"
        join_consumer_order_datetime(spark, order_fpath, save_fpath, prefix)
    return

def join_consumer_fraud(spark: SparkSession, open_folder:str, save_folder:str, start_idx:int, num_files:int=50, prefix:str="") -> None:
    """
    Join the consumer transactions with the consumer dataframes
    - Parameters
        - spark: Spark session
        - open_folder: Folder to open
        - save_folder: Folder to save
        - start_idx: Start index
        - num_files: Number of files to join
        - prefix: Prefix for the save file
    - Returns
        - None
    """

    # First we extract each of the files
    order_files_list = []
    order_names_list = []
    if open_folder != CURATED_DIR:
        order_names_list = [fname for fname in os.listdir(prefix+open_folder) if ORDER_DATETIME in fname]
        order_files_list = [f"{prefix}{open_folder}/{fname}" for fname in os.listdir(prefix+open_folder) if ORDER_DATETIME in fname]
    else:
        transaction_folders = [f"{prefix}{open_folder}{fname}" for fname in os.listdir(prefix+open_folder) \
                        if TRANSACTIONS+"_" in fname]
        for folder in transaction_folders:
            order_names = [fname for fname in os.listdir(folder) if ORDER_DATETIME in fname]
            order_files = [f"{folder}/{fname}" for fname in os.listdir(folder) if ORDER_DATETIME in fname]
            order_files_list += order_files
            order_names_list += order_names
    order_files_list = sorted(order_files_list)
    order_names_list = sorted(order_names_list)

    # Now we only want to get a number of files from a certain index
    order_files = order_files_list[start_idx:(start_idx+num_files)]
    order_names = order_names_list[start_idx:(start_idx+num_files)]

    # Now iterate and do desired operations
    for i in range(len(order_files)):
        order_fpath = order_files[i]
        order_fname = order_names[i]
        save_fpath = f"{prefix}{save_folder}{order_fname}"
        join_consumer_order_datetime(spark, order_fpath, save_fpath, prefix)
    return

# FINAL JOIN FUNCTIONS
def join_batch_merchant_transactions(start_idx:int=0, end_idx:int=NUM_TRANSACTIONS, num_per_loop:int=50, open_folder:str=CURATED_TRANSACTIONS_CURATED_PATH, 
                               save_folder:str=CURATED_TRANSACTIONS_MERCHANTS_PATH, prefix:str="" ) -> None:
    """
    Join the transactions with the merchants
    - Parameters
        - start_idx: Start index
        - end_idx: End index
        - num_per_loop: Number of files to join per loop
        - open_folder: Folder to open
        - save_folder: Folder to save
        - prefix: Prefix for the save file
    - Returns
        - None
    """

    # First make a new directory for curated transactions
    if not os.path.exists(prefix+save_folder):
        os.makedirs(prefix+save_folder)

    spark = create_spark()

    # Calculate the total length of the transactions, adjust end index if needed
    if open_folder != CURATED_DIR:
        transaction_folders_len = len([os.listdir(prefix+open_folder)])
    else:
        transaction_folders = [f"{prefix}{open_folder}{fname}" for fname in os.listdir(prefix+open_folder) \
                        if TRANSACTIONS+"_" in fname]
        transaction_folders_len = sum([len(os.listdir(folder_name)) for folder_name in transaction_folders])
        end_idx = min(end_idx, transaction_folders_len)
    curr_idx = start_idx
    while curr_idx < end_idx:
        join_merchant_transactions(spark, open_folder,
                                save_folder,
                                curr_idx,
                                num_per_loop,
                                prefix)
        curr_idx += num_per_loop
    return

def join_batch_consumer_transactions(start_idx:int=0, end_idx:int=NUM_TRANSACTIONS, num_per_loop:int=50,
                               open_folder:str=CURATED_TRANSACTIONS_MERCHANTS_PATH, save_folder:str=CURATED_TRANSACTIONS_ALL_PATH, prefix:str="") -> None:
    """
    Join the transactions with the consumers
    - Parameters
        - start_idx: Start index
        - end_idx: End index
        - num_per_loop: Number of files to join per loop
        - open_folder: Folder to open
        - save_folder: Folder to save
        - prefix: Prefix for the save file
    - Returns
        - None
    """

    # First make a new directory for curated transactions
    if not os.path.exists(prefix+save_folder):
        os.makedirs(prefix+save_folder)

    spark = create_spark()
    # Calculate the total length of the transactions, adjust end index if needed
    if open_folder != CURATED_DIR:
        transaction_folders_len = len(os.listdir(prefix+open_folder))
    else:
        transaction_folders = [f"{prefix}{open_folder}{fname}" for fname in os.listdir(prefix+open_folder) \
                        if TRANSACTIONS+"_" in fname]
        transaction_folders_len = sum([len(os.listdir(folder_name)) for folder_name in transaction_folders])
    end_idx = min(end_idx, transaction_folders_len)
    curr_idx = start_idx
    while curr_idx < end_idx:
        join_consumer_transactions(spark, open_folder,
                                save_folder,
                                curr_idx,
                                num_per_loop,
                                prefix)
        curr_idx += num_per_loop
    return

# FINAL FUNCTIONS FOR DATASETS
def join_order_datetime(spark:SparkSession, order_fpath:str, save_path:str, prefix:str="") -> None:
    """
    Join the order datetime with the order file
    - Parameters
        - spark: Spark session
        - order_fpath: Order datetime file path
        - save_path: Save path
        - prefix: Prefix for the save file
    - Returns
        - None
    """

    # REPARTITION_VALUE = 200
    order_file = read_order_datetime_file(order_fpath, spark)
    tbl_merchant = read_curated_tbl_merchant(spark, prefix)
    consumer_details = read_curated_consumer_external_join(spark, prefix).drop(NAME, ADDRESS, CONSUMER_ID)\

    # Now join the dataframes
    order_file = join_df(order_file, consumer_details, [USER_ID], INNER_JOIN)
    order_file = join_df(order_file, tbl_merchant, [MERCHANT_ABN], INNER_JOIN)
    load(PARQUET, order_file, save_path)


def join_datasets(spark: SparkSession, open_folder:str, save_folder:str, start_idx:int, num_files:int=50, prefix:str="") -> None:
    """
    Join the datasets
    - Parameters
        - spark: Spark session
        - open_folder: Folder to open
        - save_folder: Folder to save
        - start_idx: Start index
        - num_files: Number of files to join
        - prefix: Prefix for the save file
    - Returns
        - None
    """

    # First we extract each of the files
    order_files_list = []
    order_names_list = []
    if open_folder != CURATED_DIR:
        order_names_list = [fname for fname in os.listdir(prefix+open_folder) if ORDER_DATETIME in fname]
        order_files_list = [f"{prefix}{open_folder}/{fname}" for fname in os.listdir(prefix+open_folder) if ORDER_DATETIME in fname]
    else:
        transaction_folders = [f"{prefix}{open_folder}{fname}" for fname in os.listdir(prefix+open_folder) \
                        if TRANSACTIONS+"_" in fname]
        for folder in transaction_folders:
            order_names = [fname for fname in os.listdir(folder) if ORDER_DATETIME in fname]
            order_files = [f"{folder}/{fname}" for fname in os.listdir(folder) if ORDER_DATETIME in fname]
            order_files_list += order_files
            order_names_list += order_names
    order_files_list = sorted(order_files_list)
    order_names_list = sorted(order_names_list)

    # Now we only want to get a number of files from a certain index
    order_files = order_files_list[start_idx:(start_idx+num_files)]
    order_names = order_names_list[start_idx:(start_idx+num_files)]

    # Now iterate and do desired operations
    for i in range(len(order_files)):
        order_fpath = order_files[i]
        order_fname = order_names[i]
        save_fpath = f"{prefix}{save_folder}{order_fname}"
        join_order_datetime(spark, order_fpath, save_fpath, prefix)
    return

def join_batch_datasets(start_idx:int=0, end_idx:int=NUM_TRANSACTIONS, num_per_loop:int=50,
                    open_folder:str=CURATED_TRANSACTIONS_CURATED_PATH, save_folder:str=CURATED_TRANSACTIONS_ALL_PATH, prefix:str="") -> None:
    """
    Join the transactions with the consumers
    - Parameters
        - start_idx: Start index
        - end_idx: End index
        - num_per_loop: Number of files to join per loop
        - open_folder: Folder to open
        - save_folder: Folder to save
        - prefix: Prefix for the save file
    - Returns
        - None
    """

    # First make a new directory for curated transactions
    if not os.path.exists(prefix+save_folder):
        os.makedirs(prefix+save_folder)

    spark = create_spark()
    # Calculate the total length of the transactions, adjust end index if needed
    if open_folder != CURATED_DIR:
        transaction_folders_len = len(os.listdir(prefix+open_folder))
    else:
        transaction_folders = [f"{prefix}{open_folder}{fname}" for fname in os.listdir(prefix+open_folder) \
                        if TRANSACTIONS+"_" in fname]
        transaction_folders_len = sum([len(os.listdir(folder_name)) for folder_name in transaction_folders])
    end_idx = min(end_idx, transaction_folders_len)
    curr_idx = start_idx
    while curr_idx < end_idx:
        join_datasets(spark, open_folder,
                                save_folder,
                                curr_idx,
                                num_per_loop,
                                prefix)
        curr_idx += num_per_loop
    return