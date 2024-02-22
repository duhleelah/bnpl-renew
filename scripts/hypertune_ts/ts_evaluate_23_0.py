from ..constants import *
from ..read import *
from ..model_ts import *

if __name__ == "__main__":
    spark = create_spark()
    train_df = read_train_data_ts(spark).toPandas()
    test_df = read_test_data_ts(spark)
    test_df = test_df.sample(withReplacement=True, fraction=1.0)
    test_df = test_df.toPandas()
    hyperparameters_ts(train_df, test_df, "./data/curated/hypertune_ts_results.txt", "dollar_value", "")
