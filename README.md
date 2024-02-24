# Generic Buy Now, Pay Later Project

** Update: ** Repo is being updated as of 24 Feb 2024 to revisit this project

## Selects the top 100 merchants from a given Buy Now Pay Later Dataset

## HOW TO RUN

### ETL

- Run `python3 -m scripts.make_run_file`
- IF MAC OR LINUX (MEANS WSL ON WINDOWS ALSO):
  - `chmod +x ./run_files.sh`
  - `./run_files.sh`
- IF WINDOWS:
  - `chmod +x ./run_files.bat`
  - `./run_files.bat`

### MODELLING FOR LINEAR REGRESSION

- Direct to the `notebooks/base_linear_model.ipynb` notebook
  - Then, click `Run All`
  - To run hyperparameter tuning, please uncomment the `perform_hyperparams_tuning_lr(train_vec, test_vec)` line

### MODELLING FOR FRAUD PROBABILITY

- Direct to the `notebooks/predicting_merchant_fraud_probability.ipynb` notebook
  - Then, click `Run All`

### DELETE USED ETL FILES

- Copy the text from `delete_partitions.txt`
- Paste into console and run command

---

## DATASET DESCRIPTIONS

- `tables`: stores all the landing data
- `raw`: stores all the raw data (column casing + renaming)
- `curated`: stores all our curated data (joins + filtering + column selection + business rules)

### ETL FILES

- The following are the data files present in each of the folders:
  - Landing (Used the provided Tables directory as landing)
    - External Files - In this case is the Australian Buereau of Statistics (ABS) dataset pertaining to the number of earners, earnings, and age of male and female Australian consumers from 2015-2020
    - Transactions snapshot folders
    - `consumer_fraud_probability.csv`
    - `merchant_fraud_probability.csv`
    - `consumer_user_details.parquet`
    - `tbl_consumer.csv`
    - `tbl_merchants.parquet`
  - Raw
    - `raw_transactions` (combined 3 folders from landing)
    - `consumer_fraud_probability.csv`
    - `merchant_fraud_probability.csv`
    - `consumer_user_details.parquet`
    - `tbl_consumer.csv`
    - `tbl_merchants.parquet`
    - External Files
  - Curated
    - `transactions_all`: Joins external data + internal data + transactions data (INNER JOIN)
    - `tbl_merchants.parquet`
    - `consumer_joined.parquet`: Join `tbl_consumer.csv` and `consumer_user_details.parquet`
    - `consumer_external_join.parquet`: Join consumer_joined and external_joined
    - External Files
