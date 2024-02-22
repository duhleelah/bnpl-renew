from ..constants import *
from ..read import *
from ..model_rfr import *

if __name__ == "__main__":
    spark = create_spark()
    train_df = read_train_data_rfr(spark, 0, 4)
    fit_rfr(train_df, "./data/curated/hypertune_rfr_models/hypertune_rfr_models_8_0/", "./data/curated/hypertune_rfr_models/count_8_0.txt", "dollar_value", "prediction_dollar_value_0", 10, 20)