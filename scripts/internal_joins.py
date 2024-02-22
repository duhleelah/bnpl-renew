from .read import *
from .constants import *
from .join import *

def internal_extra_joins(prefix:str="") -> None:
    """
    Function is primarily used to join internal datasets
    - Parameters
        - prefix: Prefix used for access in other directories
    - Returns
        - None
    """

    spark = create_spark()
    consumer_user_details = read_raw_consumer_user_details(spark, prefix)
    tbl_consumer = read_raw_tbl_consumer(spark, prefix)

    # Join consumer user datails with table consumer data
    print("JOINING INTERNAL DATASETS...")
    joined_consumer_data = join_consumer_tbl_details(tbl_consumer, consumer_user_details)
    joined_consumer_data = joined_consumer_data.dropna()

    print("FINISHED JOINING INTERNAL DATASETS")

    # Save this dataset first before joining with external join
    print("SAVING JOINED DATASETS...")
    load(PARQUET, joined_consumer_data, CURATED_CONSUMER_JOINED_DATA, prefix)
    print("FINISHED SAVING JOINED DATASETS")


    print("JOINING EXTERNAL DATA AND CONSUMER DETAILS...")
    spark = create_spark()
    external_join = read_curated_external_join(spark, prefix)
    consumer_join = read_curated_consumer_join(spark, prefix)
    joined_consumer_external_data = join_consumer_external(consumer_join, external_join)
    print("SAVING EXTERNAL DATA AND CONSUMER DETAILS...")
    load(PARQUET, joined_consumer_external_data, CURATED_EXTERNAL_JOIN_PATH, prefix)
    print("FINISHED JOINING EXTERNAL DATA AND CONSUMER DETAILS")
    return

if __name__ == "__main__":
    internal_extra_joins()