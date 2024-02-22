from ..constants import *
from ..read import *
from ..model_neuralnet import *

if __name__ == "__main__":
    spark = create_spark()
    train_df = read_train_data_neuralnet(spark).toPandas()
    test_df = read_test_data_neuralnet(spark)
    test_df = test_df.sample(withReplacement=True, fraction=1.0)
    test_df = test_df.toPandas()
    hyperparameters_neuralnet(train_df, test_df, "{CURATED_DIR}hypertune_neuralnet_results.txt", "dollar_value", "")
