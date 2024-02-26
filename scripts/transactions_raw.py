from .constants import *
from .extract import *
from .transform import *
from .load import *
from .read import *
from .join import *
from .misc_changes import *
import argparse

def internal_raw_transactions_old(prefix:str="") -> None:
    spark = create_spark()
    print("EXTRACTING TRANSACTIONS...")
    transaction_dict = extract_transactions(spark, prefix)
    print("FINISHED EXTRACTING TRANSACTIONS")

    # Rename Columns
    print("RENAMING TRANSACTIONS COLUMNS...")
    transaction_dict = rename_transactions(transaction_dict, TRANSACTIONS_COLS_DICT)
    print("FINISHED RENAMING TRANSACTIONS COLUMNS")

    # Save Raw Files
    print("SAVING RAW TRANSACTIONS FILES...")
    load_transactions(RAW_DIR, transaction_dict, prefix)
    print("FINISHED SAVING RAW TRANSACTIONS FILES")

def internal_raw_transactions(prefix:str="") -> None:
    # First get the length of all the order_datetime files
    landing_transactions_len = get_transactions_length(TABLES_DIR)
    print("ORIGINAL TRANSACTIONS LENGTH: "+str(landing_transactions_len))

    # # Make a new folder for the raw transactions
    # if not os.path.exists(RAW_TRANSACTIONS_PATH):
    #     os.makedirs(RAW_TRANSACTIONS_PATH)

    spark = create_spark()
    for i in range(landing_transactions_len):
        curr_df, curr_fname = extract_order_file(spark, i, prefix)
        # Rename the columns
        curr_df = rename_cols(curr_df, TRANSACTIONS_COLS_DICT)
        load(PARQUET, curr_df, f"{RAW_TRANSACTIONS_PATH}{curr_fname}")
    return

# Final batch function to extract to raw files
def raw_batch_order_file(start_idx:int,
                             end_idx:int, 
                             prefix:str="") -> None:
    # print(f"GETTING RAW FILES FROM {start_idx} to {end_idx}")
    # Make a new folder for the raw transactions
    if not os.path.exists(RAW_TRANSACTIONS_PATH):
        os.makedirs(RAW_TRANSACTIONS_PATH)
    
    landing_transactions_len = get_transactions_length(TABLES_DIR)

    spark = create_spark()
    for i in range(start_idx, min(end_idx, landing_transactions_len)):
        curr_df, curr_fname = extract_order_file(spark, i, prefix)
        curr_df = rename_cols(curr_df, TRANSACTIONS_COLS_DICT)
        load(PARQUET, curr_df, f"{RAW_TRANSACTIONS_PATH}{curr_fname}")

    # TAKE 2: Instead, make the raw_transaction files

    # First get the list of all possible files
    # landing_folders = [f"{prefix}{folder}" for folder in os.listdir(TABLES_DIR) if TRANSACTIONS in folder]
    # all_files = []
    # all_names = []

    # # Now extract all the files
    # for folder in landing_folders:
    #     folder_orders = [f"{folder}/{fname}" for fname in os.listdir(folder) if ORDER_DATETIME in fname]
    #     folder_names = [fname for fname in os.listdir(folder) if ORDER_DATETIME in fname]
    #     all_files += folder_orders
    #     all_names += folder_names
    
    # # Sort the folders
    # all_files = sorted(all_files)
    # all_names = sorted(all_names)

    # # Now get the indexes you want
    # use_files = all_files[start_idx:end_idx+1]
    # use_names = all_names[start_idx:end_idx+1]

    return

if __name__ == "__main__":
    # internal_raw_transactions()
    parser = argparse.ArgumentParser(description='Process data from ith to jth dataframe')
    parser.add_argument('from_num', type=int, help='Starting index of the dataframe')
    parser.add_argument('to_num', type=int, help='Ending index of the dataframe')
    
    args = parser.parse_args()
    
    from_num = args.from_num
    to_num = args.to_num

    raw_batch_order_file(from_num, to_num)