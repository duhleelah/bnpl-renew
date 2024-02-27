from .constants import *
from .extract import *
from .transform import *
from .load import *
from .read import *
from .join import *
from .misc_changes import *

def internal_raw_files(prefix:str="") -> None:
    """
    Convert the internal files to the raw layer (excludes the transactions layer)
    - Parameters
        - prefix: Prefix used for access in other directories
    - Returns
        - None
    """

    spark = create_spark()

    ### Extract ###
    print("EXTRACTING...")
    consumer_fraud = extract(spark, prefix+CONSUMER_FRAUD_PATH)
    consumer_user_details = extract(spark, prefix+CONSUMER_USER_DETAILS_PATH)
    merchant_fraud = extract(spark, prefix+MERCHANT_FRAUD_PATH)
    tbl_consumer = extract(spark, prefix+TBL_CONSUMER_PATH)
    tbl_merchants = extract(spark, prefix+TBL_MERCHANTS_PATH)
    print("FINISHED EXTRACTING")

    # Rename Columns
    print("RENAMING COLUMNS...")
    consumer_fraud = rename_cols(consumer_fraud, CONSUMER_FRAUD_COLS_DICT)
    consumer_user_details = rename_cols(consumer_user_details, CONSUMER_USER_DETAILS_COLS_DICT)
    merchant_fraud = rename_cols(merchant_fraud, MERCHANT_FRAUD_COLS_DICT)
    tbl_consumer = rename_cols(tbl_consumer, TBL_CONSUMER_COLS_DICT)
    tbl_merchants = rename_cols(tbl_merchants, TBL_MERCHANTS_COLS_DICT)
    print("FINISHED RENAMING COLUMNS")

    # Save Raw Files
    print("SAVING RAW FILES...")
    load(CSV, consumer_fraud, prefix+RAW_CONSUMER_FRAUD_PATH)
    load(PARQUET, consumer_user_details, prefix+RAW_CONSUMER_USER_DETAILS_PATH)
    load(CSV, merchant_fraud, prefix+RAW_MERCHANT_FRAUD_PATH)
    load(CSV, tbl_consumer, prefix+RAW_TBL_CONSUMER_PATH)
    load(PARQUET, tbl_merchants, prefix+RAW_TBL_MERCHANTS_PATH)
    print("FINISHED SAVING RAW FILES")

def internal_raw_transactions(prefix:str="") -> None:
    """
    Convert the transactions layer to raw
    - Parameters
        - prefix: Prefix used for access in other directories
    - Returns
        - None
    """

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

def internal_transform_files(prefix:str="") -> None:
    """
    Transforms the interanl files, includes any further feature extractions
    from condensed features and encoding
    - Parameters
        - prefix: Used for access from other directories
    - Returns
        - None, but saves the necessary files
    """
    spark = create_spark()
    tbl_merchants = read_raw_tbl_merchants(spark, prefix)

    # Now do the dataset modifications you need to
    print("TRANSFORMING NEEDED FILES...")
    tbl_merchants = extract_from_tags(tbl_merchants)
    tbl_merchants = encode_revenue_level(tbl_merchants)
    print("FINISHED TRANSFORMING NEEDED FILES")

    print("SAVING TRANSFORMED DATASETS...")
    load(PARQUET, tbl_merchants, CURATED_TBL_MERCHANTS_PATH, prefix)
    print("FINISHED SAVING TRANSFORMED DATASETS")
    return

def internal_transform_files_2(prefix:str="") -> None:
    """
    Convert the internal files to the raw layer (excludes the transactions layer)\n
    Excludes joining external data
    - Parameters
        - prefix: Used for access in different directories
    - Returns
        - None, just saves the necessary datasets
    """
    
    spark = create_spark()
    consumer_fraud = read_raw_consumer_fraud(spark, prefix)
    consumer_user_details = read_raw_consumer_user_details(spark, prefix)
    merchant_fraud = read_raw_merchant_fraud(spark, prefix)
    tbl_consumer = read_raw_tbl_consumer(spark, prefix)
    tbl_merchants = read_raw_tbl_merchants(spark, prefix)
    # We can already join the external dataset
    # external_join = read_curated_external_join(spark, prefix)

    # Now do the dataset modifications you need to
    print("TRANSFORMING NEEDED FILES...")
    tbl_merchants = extract_from_tags(tbl_merchants)
    tbl_merchants = encode_revenue_level(tbl_merchants)
    print("FINISHED TRANSFORMING NEEDED FILES")

    print("SAVING TRANSFORMED DATASETS...")
    load(PARQUET, consumer_fraud, CURATED_CONSUMER_FRAUD_PATH, prefix)
    load(PARQUET, consumer_user_details, CURATED_CONSUMER_USER_DETAILS_PATH, prefix)
    load(PARQUET, merchant_fraud, CURATED_MERCHANT_FRAUD_PATH, prefix)
    load(PARQUET, tbl_merchants, CURATED_TBL_MERCHANTS_PATH, prefix)
    load(PARQUET, tbl_consumer, CURATED_TBL_CONSUMER_PATH, prefix)
    print("FINISHED SAVING TRANSFORMED DATASETS")
    return

def run_etl(prefix:str=""):
    """
    Runs the whole ETL involving the BNPL data excluding the gargantuan transactions data
    - Parameters
        - prefix: Used for access from other directories
    - Returns
        - None, but saves all the necessary datasets
    """
    ### Extract (Includes conversion to Raw, can pretty much just be 1 step in this case)###
    internal_raw_files(prefix)

    ### Transformation ###
    internal_transform_files(prefix)
    return

if __name__ == "__main__":
    run_etl()