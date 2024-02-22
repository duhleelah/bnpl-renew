from .constants import *
from .extract import *
from .transform import *
from .load import *
from .read import *
from .join import *
from .misc_changes import *
from .ohe import *

DATE_SPLIT = "="
DATE_IDX = 1

def internal_transform_transactions(prefix:str="") -> None:
    """
    Convert the transactions layer to raw
    - Parameters
        - prefix: Prefix used for access in other directories
    - Returns
        - None
    """
    spark = create_spark()
    raw_transactions_len = get_transactions_length(RAW_DIR)
    print("RAW DIR LEN: "+str(raw_transactions_len))
    # All we really want is the date as a column in each of the dataframes
    print("LOADING RAW TRANSACTIONS FILES...")
    transactions_dict = read_raw_transaction_files(spark, prefix)
    print("FINISHED LOADING RAW TRANSACTIONS FILES")

    # Then save the files
    print("SAVING CURATED TRANSACTION FILES...")
    load_transactions(CURATED_DIR, transactions_dict, prefix)
    print("FINISHED SAVING CURATED TRANSACTION FILES")
    return

# Final batch function to extract to raw files
def transform_batch_order_file(start_idx:int,
                             end_idx:int, 
                             prefix:str="") -> None:
    """
    Transform the batch files from the raw layer to the curated layer
    - Parameters
        - start_idx: Start index of the batch files
        - end_idx: End index of the batch files
        - prefix: Prefix used for access in other directories
    - Returns
        - None
    """
    # Make a new folder for the raw transactions
    # print(f"TRANSFORMING BATCH FILES FROM {start_idx} to {end_idx}")
    if not os.path.exists(CURATED_TRANSACTIONS_ALL_PATH):
        os.makedirs(CURATED_TRANSACTIONS_ALL_PATH)

    raw_transactions_len = get_transactions_length(RAW_TRANSACTIONS_PATH, False,
                                                   prefix)

    spark = create_spark()

    # TAKE 2: Combine the files and do the joins simultaneously
    final_df = None
    start_date = None
    to_date = None

    for i in range(start_idx, min(raw_transactions_len, end_idx)):
        # print(i)
        curr_df, curr_fname = read_index_order_datetime_file(RAW_TRANSACTIONS_PATH,
                                                             i,
                                                             spark,
                                                             prefix=prefix)
        # Add needed transformations here
        curr_df = round_dollar_values(curr_df)
        curr_df = extract_datetime_order(curr_df, curr_fname, prefix)

        # Now either assign or merge the df
        # Also store the from and to date
        if i == start_idx:
            final_df = curr_df
            start_date = curr_fname.split(DATE_SPLIT)[DATE_IDX]
        elif i == end_idx-1 or i == raw_transactions_len:
            to_date = curr_fname.split(DATE_SPLIT)[DATE_IDX]
        else:
            final_df = union_all(final_df, curr_df)

    # Now we can do the joins
    consumer_external = read_curated_consumer_external_join(spark, prefix).drop(NAME, ADDRESS, CONSUMER_ID)
    tbl_merchant = read_curated_tbl_merchant(spark, prefix)

    final_df = join_df(final_df, consumer_external, [USER_ID], INNER_JOIN)
    final_df = join_df(final_df, tbl_merchant, [MERCHANT_ABN], INNER_JOIN)

    # Now do the other needed changes and OHE
    final_df = final_df.dropna()
    for col in SMALL_CATEGORIES:
        final_df = ohe_reduce_cols(final_df, col)

    # Save the file
    print("SAVING")
    FNAME_FORMAT = "order_datetime={}_to_{}"
    new_fname = FNAME_FORMAT.format(start_date, to_date)
    curr_save_path = f"{prefix}{CURATED_TRANSACTIONS_ALL_PATH}{new_fname}"
    # print(curr_save_path)
    load(PARQUET, final_df, curr_save_path, prefix)

    return

# Same but only gets the curated data of transactions, does no joins or OHE
def transform_batch_order_file_2(start_idx:int,
                             end_idx:int, 
                             prefix:str="") -> None:
    """
    Transform the batch files from the raw layer to the curated layer
    - Parameters
        - start_idx: Start index of the batch files
        - end_idx: End index of the batch files
        - prefix: Prefix used for access in other directories
    - Returns
        - None
    """
    if not os.path.exists(CURATED_TRANSACTIONS_CURATED_PATH):
        os.makedirs(CURATED_TRANSACTIONS_CURATED_PATH)

    raw_transactions_len = get_transactions_length(RAW_TRANSACTIONS_PATH, False,
                                                   prefix)

    spark = create_spark()

    # TAKE 2: Combine the files and do the joins simultaneously
    final_df = None
    start_date = None
    to_date = None

    for i in range(start_idx, min(raw_transactions_len, end_idx)):
        # print(i)
        curr_df, curr_fname = read_index_order_datetime_file(RAW_TRANSACTIONS_PATH,
                                                             i,
                                                             spark,
                                                             prefix=prefix)
        # Add needed transformations here
        curr_df = round_dollar_values(curr_df)
        curr_df = extract_datetime_order(curr_df, curr_fname, prefix)

        # Now either assign or merge the df
        # Also store the from and to date
        if i == start_idx:
            final_df = curr_df
            start_date = curr_fname.split(DATE_SPLIT)[DATE_IDX]
        elif i == end_idx-1 or i == raw_transactions_len:
            to_date = curr_fname.split(DATE_SPLIT)[DATE_IDX]
        else:
            final_df = union_all(final_df, curr_df)

    # Save the file
    FNAME_FORMAT = "order_datetime={}_to_{}"
    new_fname = FNAME_FORMAT.format(start_date, to_date)
    curr_save_path = f"{prefix}{CURATED_TRANSACTIONS_CURATED_PATH}{new_fname}"
    # print(curr_save_path)
    load(PARQUET, final_df, curr_save_path, prefix)
    return

if __name__ == "__main__":
    # internal_transform_transactions()
    transform_batch_order_file(0, 10)