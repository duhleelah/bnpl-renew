from .etl import *
from .constants import *
from .extract import *
from .load import *
from .transform import *
from .read import *
from .sa2_age_allocation import *
import re
import pandas as pd
import random
import json

import warnings
warnings.filterwarnings("ignore")

from pyspark.sql.functions import *
from pyspark.sql.column import *
from pyspark.sql.types import *

# start spark session
spark = create_spark()

# declare constants to be used in upcoming processes
# MALE_AGE_PATTERN = r"^male_age.+"
# FEMALE_AGE_PATTERN = r"^female_age.+"
# ALLOCATED_AGES = "allocated_ages"
# MALE = "Male"
# FEMALE = "Female"
# MALE_AGES_DICT = "male_ages"
# FEMALE_AGES_DICT = "female_ages"
# DEFAULT = 0
# IDX = 0
# male_ages = {}
# female_ages = {}


# def find_appropriate_age_columns(columns: list[str]) -> list:
#     FIND_AGE_PATTERN = r"\d{1,2}"
#     MAX_AGE = 14
#     MAX_BOUND_IDX = 0
#     return_column = []

#     for column in columns:
#         age_bounds = re.findall(FIND_AGE_PATTERN, column)

#         if int(age_bounds[MAX_BOUND_IDX]) >= MAX_AGE:
#             return_column.append(column)

#     return return_column

# def save_age_proportions(df: DataFrame, columns: list[str], name: str) -> dict:
#     age_dict = {}
#     IDX = 0
#     NULL_EXISTS = 1
#     sa2_areas = list(df[SA2_CODE])
#     null_flag = 0

#     for area in sa2_areas:
#         age_dict[area] = {}
#         area_row = df[df[SA2_CODE] == area]
#         people_in_area = area_row[columns].sum(axis=1).iloc[IDX]
#         if not people_in_area:
#             people_in_area = DEFAULT
#             null_flag = NULL_EXISTS
#         for column in columns:
#             if null_flag:
#                 age_dict[area][column] = DEFAULT
#             else:
#                 people_in_age_group = area_row[column].iloc[IDX]
#                 age_proportion = (people_in_age_group / people_in_area)
#                 age_dict[area][column] = age_proportion
#         null_flag = DEFAULT

#     with open(CURATED_DIR+name+JSON, "w") as json_file:
#         json.dump(age_dict, json_file)

#     return age_dict


# def replace_column_value(df: DataFrame, columns: list[str]) -> DataFrame:
#     for column in columns:
#         df = df.withColumn(column, lit(DEFAULT))
#     return df


# def alloc_male_age(area: int) -> str:
#     SAMPLE_AMT = 1

#     with open(CURATED_DIR+MALE_AGES_DICT+JSON, "r") as json_file:
#         male_ages = json.load(json_file)

#     area_data = male_ages.get(area, {})
#     cols = list(area_data.keys())
#     props = list(area_data.values())
#     alloc_age = random.choices(cols, weights=props, k=SAMPLE_AMT)

#     return alloc_age[IDX]


# def alloc_female_age(area: int) -> str:
#     SAMPLE_AMT = 1

#     with open(CURATED_DIR+FEMALE_AGES_DICT+JSON, "r") as json_file:
#         female_ages = json.load(json_file)

#     area_data = female_ages.get(area, {})
#     cols = list(area_data.keys())
#     props = list(area_data.values())
#     alloc_age = random.choices(cols, weights=props, k=SAMPLE_AMT)

#     return alloc_age[IDX]


# allocate_male_age_udf = udf(alloc_male_age, StringType())
# allocate_female_age_udf = udf(alloc_female_age, StringType())


# def flag_age_group(df:DataFrame, columns: list[str]) -> DataFrame:
#     YES = 1
#     NO = 0

#     for column in columns:
#         df = df.withColumn(column,
#                         when(col(ALLOCATED_AGES) == column, YES)\
#                         .otherwise(NO))

#     return df


# def perform_age_allocation():
#     # read in the external data and external joined with consumer data for
#     # allocating age group to each consumer
#     consumer_and_external = read_curated_consumer_external_join(spark)
#     external_data = pd.read_csv(CURATED_EXTERNAL_JOIN_PATH)
#     external_data[SA2_CODE] = external_data[SA2_CODE].astype(str)

#     male_columns = list(external_data.filter(
#         regex=MALE_AGE_PATTERN, axis=1).columns)
#     female_columns = list(external_data.filter(
#         regex=FEMALE_AGE_PATTERN, axis=1).columns)
#     all_age_columns = male_columns + female_columns

#     male_columns = find_appropriate_age_columns(male_columns)
#     female_columns = find_appropriate_age_columns(female_columns)

#     male_ages = save_age_proportions(external_data,
#                                      male_columns, MALE_AGES_DICT)
#     female_ages = save_age_proportions(external_data,
#                                        female_columns, FEMALE_AGES_DICT)

#     consumer_and_external = replace_column_value(
#         consumer_and_external, all_age_columns)

#     male_consumers = consumer_and_external.where(col(GENDER) == MALE)
#     female_consumers = consumer_and_external.where(col(GENDER) == FEMALE)

#     male_consumers = male_consumers.withColumn(
#         ALLOCATED_AGES, allocate_male_age_udf(col(SA2_CODE)))
#     female_consumers = female_consumers.withColumn(
#         ALLOCATED_AGES, allocate_female_age_udf(col(SA2_CODE)))

#     male_consumers = flag_age_group(male_consumers, male_columns)\
#         .drop(ALLOCATED_AGES)
#     female_consumers = flag_age_group(female_consumers, female_columns)\
#         .drop(ALLOCATED_AGES)

#     final_consumer_and_external = male_consumers.union(female_consumers)

#     return final_consumer_and_external

# start spark session
spark = create_spark()
df1, cols1, df2, cols2 = perform_age_allocation(spark)

ult_df = df1.union(df2)
print(ult_df.select(*(cols2)).where(col(GENDER) == "Female"))
