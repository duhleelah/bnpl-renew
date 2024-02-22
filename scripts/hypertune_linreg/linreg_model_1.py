from ..constants import *
from ..read import *
from ..model_linreg import *

if __name__ == "__main__":
    spark = create_spark()
    train_df = read_train_data(spark)
    fit_linreg(train_df, "./data/curated/hypertune_linreg_models/hypertune_linreg_models_1/", "dollar_value", 0.25, 0.001)
