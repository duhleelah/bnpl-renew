from ..constants import *
from ..read import *
from ..model_rfr import *

if __name__ == "__main__":
    spark = create_spark()
    test_df = read_test_data(spark)
    test_df = test_df.sample(withReplacement=True, fraction=1.0)
    predict_rfr(test_df, "./data/curated/hypertune_rfr_models/hypertune_rfr_models_2_0/", "./data/curated/hypertune_rfr_models/predictions_2_0.txt", "dollar_value", "prediction_dollar_value_0", "")
    predict_rfr(test_df, "./data/curated/hypertune_rfr_models/hypertune_rfr_models_2_1/", "./data/curated/hypertune_rfr_models/predictions_2_1.txt", "dollar_value", "prediction_dollar_value_1", "")
    calculate_rmse(test_df, "dollar_value", "./data/curated/hypertune_rfr_results.txt", ['./data/curated/hypertune_rfr_models/predictions_2_0.txt', './data/curated/hypertune_rfr_models/predictions_2_1.txt'], ['./data/curated/hypertune_rfr_models/count_2_0.txt', './data/curated/hypertune_rfr_models/count_2_1.txt'], "")