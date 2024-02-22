from .constants import *
from .load import *

REDUCE_COPY_NAME_FORMAT = "reduce_copy_file_{}.py"
REDUCE_COPY_FILE_FORMAT = """from .constants import *
from .read import *
from .load import *
from .divide_dfs import *

if __name__ == "__main__":
    reduce_copy_file({}, {}, {}, {}, {})
"""

MERCHANT_FILE_FORMAT = "transactions_merchants_{}.py"
MERCHANT_JOIN_FORMAT= "from .constants import *\nfrom .join import *\n\nif __name__ == \"__main__\":\n\tjoin_batch_merchant_transactions({}, {}, {})"

CONSUMER_FILE_FORMAT = "transactions_consumers_{}.py"
CONSUMER_JOIN_FORMAT= "from .constants import *\nfrom .join import *\n\nif __name__ == \"__main__\":\n\tjoin_batch_consumer_transactions({}, {}, {})"

WHOLE_JOIN_NAME_FORMAT = "transactions_whole_{}.py"
WHOLE_JOIN_FILE_FORMAT = """from .constants import *
from .join import *

if __name__ == "__main__":
    join_batch_datasets({}, {}, {})
"""

TRANSACTIONS_RAW_NAME_FORMAT = "transactions_raw_partition_{}.py"
TRANSACTIONS_RAW_FILE_FORMAT = """from .constants import *
from .extract import *
from .transform import *
from .load import *
from .transactions_raw import *

if __name__ == "__main__":
    raw_batch_order_file({}, {})"""

TRANSACTIONS_TRANSFORM_NAME_FORMAT = "transactions_transform_partition_{}.py"
TRANSACTIONS_TRANSFORM_FILE_FORMAT = """from .extract import *
from .transform import *
from .load import *
from .transactions_transform import *

if __name__ == "__main__":
    transform_batch_order_file({}, {})"""

TRANSACTIONS_TRANSFORM_MIN_NAME_FORMAT = "transactions_transform_partition_min_{}.py"
TRANSACTIONS_TRANSFORM_MIN_FILE_FORMAT = """from .extract import *
from .transform import *
from .load import *
from .transactions_transform import *

if __name__ == "__main__":
    transform_batch_order_file_2({}, {})"""


CONSUMER_EXTERNAL_JOIN_PARTITION_MAKE_NAME_FORMAT = "consumer_external_join_partition_make_{}.py"
CONSUMER_EXTERNAL_JOIN_PARTITION_MAKE_FILE_FORMAT = """from .constants import *
from .load import *
from .divide_dfs import *

if __name__ == "__main__":
    divide_single_file({}, {}, {}, {}, {})"""

TRANSACTIONS_OHE_PARTITION_NAME_FORMAT = "transaction_ohe_partition_{}_{}.py"
TRANSACTIONS_OHE_PARTITION_FILE_FORMAT = """from .constants import*
from .load import *
from .ohe import *
from .read import *

if __name__ == "__main__":
    spark = create_spark()
    df, df_fname = read_index_order_datetime_file("{}", {}, spark)
    df = ohe_from_txt(df, "{}", "{}")
    save_path = "{}" + df_fname
    load("{}", df, save_path)
"""

TRANSACTIONS_DISTINCT_PARTITION_NAME_FORMAT = "transactions_distinct_{}.py"
TRANSACTIONS_DISTINCT_PARTITION_FILE_FORMAT = """from .constants import *
from pyspark.sql import functions as F

if __name__ == "__main__":
    spark = create_spark()
    transactions_all = spark.read.parquet("{}*", header=True)
    distinct_vals = [row["{}"] for row in transactions_all.select(F.col("{}")).distinct().collect()]
    with open("{}", "w") as fp:
        for val in distinct_vals:
            fp.write(str(val)+"\\n")
"""

# Another file format for partitioning the columns further, happens when the OHE 
TRANSACTIONS_DISTINCT_PARTITION_NAME_DIVIDED_FORMAT = "transactions_distinct_{}_{}.py"
TRANSACTIONS_DISTINCT_PARTITION_FILE_DIVIDED_FORMAT = """from .constants import *
from pyspark.sql import functions as F

if __name__ == "__main__":
    spark = create_spark()
    transactions_all = spark.read.parquet("{}*", header=True)
    distinct_vals = [row["{}"] for row in transactions_all.select(F.col("{}")).distinct().collect()]
    distinct_vals = distinct_vals[{}:{}]
    with open("{}", "w") as fp:
        for val in distinct_vals:
            fp.write(str(val)+"\\n")
"""

# Final OHE Scripts, one for big columns (OneHotEncoder), and another just for 
TRANSACTIONS_OHE_SMALL_NAME_FORMAT = "transactions_ohe_small_{}.py"
TRASNACTIONS_OHE_SMALL_FILE_FORMAT = """from .constants import *
from .ohe import *

if __name__ == "__main__":
    ohe_small_cols_batch({}, {})
"""

TRANSACTIONS_OHE_BIG_NAME_FORMAT = "transactions_ohe_big_{}.py"
TRASNACTIONS_OHE_BIG_FILE_FORMAT = """from .constants import *
from .ohe import *

if __name__ == "__main__":
    ohe_big_cols_batch({}, {})
"""

TRANSACTIONS_BATCH_NAME_FORMAT = "transactions_batch_{}.py"
TRANSACTIONS_BATCH_FILE_FORMAT = """from .constants import *
from .misc_changes import *

if __name__ == "__main__":
    save_assign_batch_df({}, {})
"""

def make_partitioned_transactions_raw(folder:str=TABLES_DIR,
                                      save_folder:str=SCRIPTS_DIR,
                                      prefix:str="") -> None:
    """
    Partitions the conversion of transaction files to the raw layer
    - Parameters
        - folder     : Folder to base the total number of transactions from
        - save_folder: Folder to save the files running the conversion from landing to raw data
        - prefix     : Used for access from different directories
    - Returns
        - None, but saves the needed files for transaction data's conversion to raw layer
    """
    # First get all the possible number of files
    landing_transactions_length = get_transactions_length(folder, prefix=prefix)
    raw_idx = 0

    # Now loop and make a new script each time
    while raw_idx <= landing_transactions_length:
        current_fname = prefix+save_folder+TRANSACTIONS_RAW_NAME_FORMAT.format(raw_idx//TRANSACTIONS_RAW_NUM_FILES)
        curr_pyfile = TRANSACTIONS_RAW_FILE_FORMAT.format(raw_idx, min(raw_idx+TRANSACTIONS_RAW_NUM_FILES, 
                                                                       landing_transactions_length))
        with open(current_fname, 'w') as fp:
            fp.write(curr_pyfile)
        raw_idx += TRANSACTIONS_RAW_NUM_FILES
    return

def make_partitioned_transactions_transformed(folder:str=TABLES_DIR,
                                      save_folder:str=SCRIPTS_DIR,
                                      prefix:str="") -> None:
    """
    Partitions the transformation of the transactions data, this includes joins with the other datasets,
    null and duplicate removal, and other miscallaneous changes to process the datasets
    - Parameters
        - folder     : Folder to base the total number of transactions from
        - save_folder: Folder to save the files running the conversion from raw to curated data
        - prefix     : Used for access from different directories
    - Returns
        - None, but saves the needed files for transaction data's conversion to curated layer
    """
    # First get all the possible number of fiels
    raw_transactions_length = get_transactions_length(folder, prefix=prefix)
    curated_idx = 0
    
    # Now loop and make a new script each time
    while curated_idx <= raw_transactions_length:
        current_fname = prefix+save_folder+TRANSACTIONS_TRANSFORM_NAME_FORMAT.format(curated_idx//TRANSACTIONS_TRANSFORM_NUM_FILES)
        curr_pyfile = TRANSACTIONS_TRANSFORM_FILE_FORMAT.format(curated_idx, min(curated_idx+TRANSACTIONS_TRANSFORM_NUM_FILES,
                                                                           raw_transactions_length))
        with open(current_fname, 'w') as fp:
            fp.write(curr_pyfile)
        curated_idx += TRANSACTIONS_TRANSFORM_NUM_FILES
    return

def make_partitioned_model_batches(folder:str=CURATED_TRANSACTIONS_ALL_PATH,
                                      save_folder:str=SCRIPTS_DIR,
                                      pyfile_folder:str=CURATED_TRANSACTIONS_ALL_BATCHED_PATH,
                                      prefix:str="") -> None:
    """
    Batch assignment of partition values to make modelling doable by batches
    - Parameters
        - folder       : Folder to base the total number of transactions from
        - save_folder  : Folder to save the files running the conversion from raw to curated data
        - pyfile_folder: Folder to input for the assign batch functio
        - prefix       : Used for access from different directories
    - Returns
        - None, but saves the needed files for transaction data's conversion to curated layer
    """
    # First get all the possible number of fiels
    curated_transactions_length = get_transactions_length(folder, is_nested=False)
    curated_idx = 0

    while curated_idx < curated_transactions_length:
        current_fname = prefix+save_folder+TRANSACTIONS_BATCH_NAME_FORMAT.format(curated_idx)
        curr_pyfile = TRANSACTIONS_BATCH_FILE_FORMAT.format(curated_idx, f"\"{pyfile_folder}\"")
        with open(current_fname, 'w') as fp:
            fp.write(curr_pyfile)
        curated_idx += 1
    return

if __name__ == "__main__":
    make_partitioned_transactions_raw()
    make_partitioned_transactions_transformed()
    make_partitioned_model_batches()