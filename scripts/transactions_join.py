from .constants import *
from .extract import *
from .transform import *
from .load import *
from .read import *
from .join import *
from .misc_changes import *
from functools import reduce
from pyspark.sql.functions import broadcast

# def join_transactions(open_folder:str=CURATED_DIR, prefix:str="") -> None:
#     spark = create_spark()

#     consumer_external_data = read_curated_consumer_external_join(spark, prefix)
#     tbl_merchants = read_curated_tbl_merchant(spark, prefix)
#     # print(consumer_external_data.columns)
#     # print(tbl_merchants.columns)

#     # Now run the whole function
#     # join_merchant_consumer_transaction_data(open_folder,
#     #                                         prefix)
#     return
def union_all(*dfs):
    return reduce(DataFrame.unionAll, dfs)

def join_transactions_merchants_partitions(iter, merchants):
    for row in iter:
        joined_row = row.join(merchants, on=[MERCHANT_ABN], how=INNER_JOIN)
        yield joined_row


def join_transactions_merchants(open_folder:str=CURATED_DIR, 
                               save_folder:str=CURATED_TRANSACTIONS_MERCHANTS_PATH,
                               prefix:str="") -> None:
    if not os.path.exists(prefix+save_folder):
        os.makedirs(prefix+save_folder)
    
    spark = create_spark()
    print("EXTRACTING THE TRANSACTION DATA")
    merchants = read_curated_tbl_merchant(spark)
    broadcast_merchants = broadcast(merchants)
    transaction_folders = [f"{prefix}{open_folder}{fname}/*" for fname in os.listdir(prefix+open_folder) \
                if TRANSACTIONS+"_" in fname]
    transaction_dfs_list = [spark.read.parquet(fname) for fname in transaction_folders]
    transaction_dfs = transaction_dfs_list[0]
    for i in range(1, len(transaction_dfs_list)):
        transaction_dfs = union_all(transaction_dfs, transaction_dfs_list[i])
    print(type(transaction_dfs))
    print("APPLYING MERCHANT JOINS")
    print("REPARTITIONING")
    transaction_dfs = transaction_dfs.repartition(MERCHANT_NUM_FILES)
    # transactions_rdd = transaction_dfs.rdd.mapPartitions(lambda x: join_transactions_merchants_partitions(x, merchants))
    print("MAPPING")
    transactions_rdd = transaction_dfs.rdd.mapPartitions(lambda x: join_transactions_merchants_partitions(x, merchants))
    print("MAKING NEW SCHEMA")
    new_schema = StructType(TRANSACTIONS_COLS_SCHEMA.fields + TBL_CONSUMER_COLS_SCHEMA.fields)
    print("CREATING NEW DF")
    # transaction_dfs = spark.createDataFrame(transactions_rdd, schema=new_schema)
    new_df = transactions_rdd.toDF(new_schema)

    print("SAVING THE BIG DATAFRAME AS A PARQUET")
    load(PARQUET, new_df, f"{CURATED_DIR}/merchant_transactions{PARQUET}")
    return

if __name__ == "__main__":
    # join_transactions()
    # print("BATCH 1")
    # join_batch_merchant_transactions(MERCHANT_BATCH_1_IDX, MERCHANT_BATCH_1_IDX+MERCHANT_NUM_FILES)
    # print("BATCH 2")
    # join_batch_merchant_transactions(MERCHANT_BATCH_2_IDX, MERCHANT_BATCH_2_IDX+MERCHANT_NUM_FILES)
    # print("BATCH 3")
    # join_batch_merchant_transactions(MERCHANT_BATCH_3_IDX, MERCHANT_BATCH_3_IDX+MERCHANT_NUM_FILES)
    # print("LENGTH OF NEW FOLDER: "+str(len(os.listdir(CURATED_TRANSACTIONS))))
    # transaction_folders = [f"{CURATED_DIR}{fname}" for fname in os.listdir(CURATED_DIR) \
    #                 if TRANSACTIONS+"_" in fname]
    # transaction_folders_len = sum([len(os.listdir(folder_name)) for folder_name in transaction_folders])
    # print("LENGTH OF ALL FILES: "+str(transaction_folders_len))
    # join_batch_merchant_transactions()
    # join_transactions_merchants()
    join_batch_consumer_transactions(open_folder=CURATED_TRANSACTIONS_MERCHANTS_PATH,
                                     save_folder=CURATED_TRANSACTIONS_ALL_PATH, 
                                     num_per_loop=20)