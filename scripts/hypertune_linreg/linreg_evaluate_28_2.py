from ..constants import *
from ..read import *
from ..model_linreg import *

if __name__ == "__main__":
    spark = create_spark()
    test_df = read_test_data(spark)
    test_df = test_df.sample(withReplacement=True, fraction=1.0)
    evaluate_linreg(test_df, "./data/curated/hypertune_linreg_models/hypertune_linreg_models_28/", "./data/curated/hypertune_linreg_results.txt", "dollar_value")
