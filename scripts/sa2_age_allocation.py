from .constants import *
from .read import *
from .load import *
import re
from pandas import *
import random
import json
import numpy as np

import warnings
warnings.filterwarnings("ignore")

from pyspark.sql.functions import when, lit, col, udf
from pyspark.sql.column import *
from pyspark.sql.types import StringType

# start spark session
spark = create_spark()

# declare constants to be used in upcoming processes
MALE_AGE_PATTERN = r"^male_age.+"
FEMALE_AGE_PATTERN = r"^female_age.+"
ALLOCATED_AGES = "allocated_ages"
MALE = "Male"
FEMALE = "Female"
MALE_AGES_DICT = "male_ages"
FEMALE_AGES_DICT = "female_ages"
DEFAULT = 0
IDX = 0

# ---------- SA2 Allocation Functions --------------- #

def attain_population_proportions(df: DataFrame, prefix) -> DataFrame:
    """
    For each postcode, identify the SA2 codes it is attributed to.
    Then, calculate the population proportions/density for each of
    the attributed postcode to use as weights. Store as a JSON file.
    - Parameters
        - df: DataFrame of population data
        - prefix: Prefix for the output file
    - Returns
        - df: DataFrame with renamed columns
    """
    POPULATIONS_DATA = "populations_data"
    AREAS = "areas"
    PROPORTIONS = "proportions"
    COLUMNS_OF_INTEREST = [POSTCODE, SA2_CODE, TOTAL_FEMALES, TOTAL_MALES]
    TOTAL_POPULATION = "total_sa2_population"
    ROW_IDX = 1
    populations_dict = dict()
    invalid_sa2_areas = list()

    # filter the dataframe to only retain columns of interest,
    # and ensure data types are valid as conversion between
    # spark and pandas can assume different types
    population_df = df[COLUMNS_OF_INTEREST].sort_values(by=POSTCODE)
    population_df = population_df[COLUMNS_OF_INTEREST].astype(float).astype(int)
    population_df[TOTAL_POPULATION] = population_df[TOTAL_MALES] + population_df[TOTAL_FEMALES]

    # list of all unique postcodes
    postcodes = list(population_df[POSTCODE].unique())

    # for each postcode, create a nested dictionary in
    # populations_dict
    for postcode in postcodes:
        postcode_data = population_df[population_df[POSTCODE] == postcode]
        # find the population of the entire postcode
        total_postcode_population = np.sum(postcode_data[TOTAL_POPULATION])
        # for each SA2 in the postcode, calculate its population proportion
        # if it no data for the SA2 exists, classify as invalid
        if not total_postcode_population:
            continue
        populations_dict[int(postcode)] = dict()
        populations_dict[postcode][AREAS] = list()
        populations_dict[postcode][PROPORTIONS] = list()
        for row in postcode_data.iterrows():
            row = row[ROW_IDX]
            sa2_area = int(row[SA2_CODE])
            sa2_population = row[TOTAL_POPULATION]
            if not sa2_population:
                invalid_sa2_areas.append(str(sa2_area))
                continue
            else:
                populations_dict[postcode][AREAS].append(sa2_area)
                populations_dict[postcode][PROPORTIONS].append(float(sa2_population / total_postcode_population))

    with open(prefix+CURATED_DIR+POPULATIONS_DATA+JSON, "w") as json_file:
        json.dump(populations_dict, json_file)

    return populations_dict, invalid_sa2_areas

def allocate_sa2_area(areas: list, proportions: list) -> int:
    """
    Allocate an SA2 area of a given consumer postcode by using 
    the calculated proportions of the population and sampling
    by weight.
    - Parameters
        - areas: List of SA2 areas
        - proportions: List of proportions of the population
    - Returns
        - allocated_area: The allocated SA2 area
    """
    IDX = 0
    SAMPLE_AMT = 1

    # allocate an area based on proportion weights
    alloc_area = random.choices(areas, weights=proportions, k=SAMPLE_AMT)

    return int(alloc_area[IDX])

allocate_sa2_area_udf = udf(allocate_sa2_area, IntegerType())


def perform_sa2_allocation(spark: SparkSession, consumer_data: DataFrame, external_data: DataFrame, prefix: str="") -> DataFrame:
    """
    Perform SA2 code allocation to each consumer in the database by the distribution of the
    population of an SA2 code within a postcode.
    - Parameters
        - spark: SparkSession object
        - consumer_data: DataFrame of consumer data
        - external_data: DataFrame of external data
        - prefix: Prefix for the output file
    - Returns
        - consumer_data: DataFrame with allocated SA2 codes
    """

    print("ATTAIN POPULATION PROPORTIONS")
    populations, invalid_areas = attain_population_proportions(external_data, prefix)
    populations_list = [{POSTCODE: key, **value} for key, value in populations.items()]
    populations_df = spark.createDataFrame(populations_list)
    print("POPULATION PROPORTIONS ATTAINED")
    print("FILTER EXTERNAL DATA WITH NO DATA ON INVALID SA2 CODES")
    external_data_postcodes = list(external_data[POSTCODE].astype(int).unique())
    external_data = spark.createDataFrame(external_data)
    external_data = external_data.filter(~col(SA2_CODE).isin(invalid_areas))

    print("FILTER CONSUMER DATA WITH NO EXTERNAL DATA ON INVALID POSTCODES")
    postcodes = consumer_data.select(POSTCODE)
    postcode_values = list(postcodes.distinct().rdd.flatMap(lambda x: x).collect())
    invalid_postcodes = [code for code in postcode_values if code in external_data_postcodes]

    invalid_postcodes = set(postcode_values).symmetric_difference(set(external_data_postcodes))
    invalid_postcodes = list(invalid_postcodes)

    print("FILTER ROWS WITH INVALID POSTCODES INFO")
    consumer_data = consumer_data.filter(~col(POSTCODE).isin(invalid_postcodes))

    print("JOIN CONSUMER DATA WITH POPULATION PROPORTIONS")
    joined_consumer_data = consumer_data.join(populations_df, on=POSTCODE, how=INNER_JOIN).drop(ADDRESS)
    print("ALLOCATE SA2 CODES")
    joined_consumer_data = joined_consumer_data.withColumn(SA2_CODE, allocate_sa2_area_udf(col("areas"), col("proportions")))
    joined_consumer_data = joined_consumer_data.drop("areas", "proportions")
    print("FINISHED ALLOCATING SA2 CODES")

    print("SAVING JOINED CONSUMER DATA")
    load(PARQUET, joined_consumer_data, CURATED_ALLOCATED_SA2_CONSUMER_PATH, prefix)

    print("FINISHED SAVING JOINED CONSUMER DATA")

    return joined_consumer_data, external_data.toPandas()

# ---------- Age Allocation Functions --------------- #

def find_appropriate_age_columns(columns: list[str]) -> list:
    """
    From a list of age columns, filter out the column names to only
    include potential consumer ages
    - Parameters
        - columns: List of column names 
    - Returns
        - return_column: List of column names that are potential consumer ages
    """
    FIND_AGE_PATTERN = r"\d{1,2}"
    MAX_AGE = 14
    MAX_BOUND_IDX = 0
    return_column = []

    # for each column, check if the age group is above the minimum
    # buyer age
    for column in columns:
        age_bounds = re.findall(FIND_AGE_PATTERN, column)

        # filtering stage
        if int(age_bounds[MAX_BOUND_IDX]) >= MAX_AGE:
            return_column.append(column)

    return return_column

def save_age_proportions(df: DataFrame, columns: list[str], name: str, prefix: str="") -> dict:
    """
    For each SA2 area, find the proportions of each age group and save it in a
    JSON file in the data/curated/ path.
    - Parameters
        - df: DataFrame of external data
        - columns: List of column names
        - name: Name of the JSON file
        - prefix: Prefix for the output file
    - Returns
        - age_dict: Dictionary of SA2 areas with age proportions
    """
    age_dict = {}
    invalid_areas = []
    IDX = 0
    AGE_GROUP = "age_group"
    PROPORTION = "proportion"
    sa2_areas = list(df[SA2_CODE])

    # for each area, find the total amount of people in that area
    for area in sa2_areas:
        area_row = df[df[SA2_CODE] == area]
        people_in_area = area_row[columns].sum(axis=1).iloc[IDX]

        # if it is null, record as invalid area
        # move onto next area
        if not people_in_area:
            invalid_areas.append(area)
            continue
        age_dict[area] = dict()
        age_dict[area][AGE_GROUP] = list()
        age_dict[area][PROPORTION] = list()
        # for each age column, find the age proportion with column
        # name as key
        for column in columns:
            people_in_age_group = area_row[column].iloc[IDX]
            age_proportion = float(people_in_age_group / people_in_area)
            age_dict[area][AGE_GROUP].append(column)
            age_dict[area][PROPORTION].append(age_proportion)

    # save dictionary as json file under /data/curated/ path
    with open(prefix+CURATED_DIR+name+JSON, "w") as json_file:
        json.dump(age_dict, json_file)

    age_list = [{SA2_CODE: int(key), **value} for key, value in age_dict.items()]
    age_proportions_df = spark.createDataFrame(age_list)

    return age_proportions_df, invalid_areas


def alloc_age(age_group: list[str], proportions: list[int]) -> StringType:
    """
    For the population, allocate an age group based on the weights
    of population proportion according to their SA2 area
    - Parameters
        - age_group: List of age groups
        - proportions: List of proportions
    - Returns
        - age_group[IDX]: Age group allocated
    """
    SAMPLE_AMT = 1
    IDX = 0

    alloc_age = random.choices(age_group, weights=proportions, k=SAMPLE_AMT)

    return alloc_age[IDX]


# transform into udf
allocate_age_udf = udf(alloc_age, StringType())


def perform_age_allocation(joined_consumer_data: DataFrame, external_data: DataFrame, prefix: str="") -> DataFrame:
    """
    Commence the entire age allocation pipeline.
    - Parameters    
        - joined_consumer_data: DataFrame of joined consumer data
        - external_data: DataFrame of external data
        - prefix: Prefix for the output file
    - Returns
        - joined_consumer_data: DataFrame of joined consumer data
        - external_data: DataFrame of external data
    """

    AGE_GROUP = "age_group"
    PROPORTION = "proportion"

    # find the list of age-related columns from external data
    print("LIST OF AGE RELATED COLUMNS")
    df_columns = list(external_data.columns)
    male_columns = list()
    female_columns = list()

    for column in df_columns:
        male_column_found = re.findall(MALE_AGE_PATTERN, column)
        if male_column_found:
            male_columns = male_columns+male_column_found

        female_column_found = re.findall(FEMALE_AGE_PATTERN, column)
        if female_column_found:
            female_columns = male_columns+female_column_found


    # for each gender, identify the relevant ages for consumers
    # e.g. adolescent and older
    print("FIND APPROPRIATE AGE COLUMNS")
    male_columns = find_appropriate_age_columns(male_columns)
    female_columns = find_appropriate_age_columns(female_columns)

    # find the age proportions for an SA2 area
    print("AGE PROPORTIONS OF SA2")
    male_proportions, male_invalid_areas = save_age_proportions(external_data,
                                     male_columns, MALE_AGES_DICT, prefix)
    female_proportions, female_invalid_areas = save_age_proportions(external_data,
                                       female_columns, FEMALE_AGES_DICT, prefix)

    # group consumers by gender and also remove consumers in areas
    # that are invalid (e.g. inhabitable)
    print("FILTER CONSUMERS BY GENDER")
    male_consumers = joined_consumer_data.where(col(GENDER) == MALE)\
        .where(~col(SA2_CODE).isin(male_invalid_areas))
    female_consumers = joined_consumer_data.where(col(GENDER) == FEMALE)\
        .where(~col(SA2_CODE).isin(female_invalid_areas))

    print("JOIN AGE PROPORTIONS BY AREA WITH CONSUMER DATA")
    male_consumers = male_consumers.join(male_proportions, on=SA2_CODE, how=INNER_JOIN)
    female_consumers = female_consumers.join(female_proportions, on=SA2_CODE, how=INNER_JOIN)

    # perform age group allocation
    print("PERFORM AGE GROUP ALLOCATION")
    male_consumers = male_consumers.withColumn(
        ALLOCATED_AGES, allocate_age_udf(col(AGE_GROUP), col(PROPORTION)))
    female_consumers = female_consumers.withColumn(
        ALLOCATED_AGES, allocate_age_udf(col(AGE_GROUP), col(PROPORTION)))

    # concatenate the two datasets together
    print("JOIN CONSUMER DATASETS TOGETHER")
    df = male_consumers.union(female_consumers)
    df = df.drop(*(AGE_GROUP, PROPORTION, CONSUMER_ID))
    print("JOIN CONSUMER DATA WITH EXTERNAL DATA")
    spark_external_data = spark.createDataFrame(external_data)
    df = df.join(spark_external_data, on=[POSTCODE, SA2_CODE, STATE], how=INNER_JOIN)

    print("SAVE DATASETS TO CURATED CONSUMER PATH")
    load(PARQUET, df, CURATED_CONSUMER_EXTERNAL_JOIN_AGE_ALLOCATED_PATH, prefix)

    return


def main_age_allocation(prefix:str="") -> None:
    """
    Main function for age allocation.
    - Parameters
        - prefix: Prefix for the output file    
    - Returns
        - None
    """
    spark = create_spark()

    # read in datasets
    external_data = pd.read_csv(prefix+CURATED_EXTERNAL_JOIN_PATH)
    external_data = external_data.astype({SA2_CODE: "str", POSTCODE: "int"})
    consumer_data = read_curated_consumer_join(spark, prefix)

    # Perform SA2 allocation
    print("ALLOCATING SA2 CODE FUNCTION RUNNING...")
    joined_consumer_data, external_data = perform_sa2_allocation(
        spark, consumer_data, external_data, prefix)

    # Perform age allocation
    print("ALLOCATING AGES FUNCTION RUNNING...")
    perform_age_allocation(joined_consumer_data, external_data, prefix)

    return


if __name__ == "__main__":
    main_age_allocation()
