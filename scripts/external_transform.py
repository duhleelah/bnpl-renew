from .constants import *
from .external_extract import *
from pyspark.sql import DataFrame
import geopandas as gpd
import pyspark.sql.functions as F
from pyspark.sql.types import IntegerType
from .external_load import load_external_data
from pandas import *

EARNING_INFO_REMOVE = 2297
SA2_TO_POSTCODE_REMOVE = 5988
MALE_REMOVE = 2458
FEMALE_REMOVE = 2458


# Renaming columns
def rename_shape_file(gdf: gpd.GeoDataFrame, mapping_dict: dict) -> gpd.GeoDataFrame:
    """
    Rename the columns of the GeoDataFrame
    - Parameters
        - gdf: Input GeoDataFrame
        - mapping_dict: Dictionary of columns names
    - Returns
        - gdf: GeoDataFrame with renamed columns
    """
    # only keep the columns we need
    columns_to_keep = ["SA2_CODE21", "SA2_NAME21", "STE_NAME21", "geometry"]
    gdf = gdf[columns_to_keep]

    # rename the columns
    gdf = gdf.rename(columns=mapping_dict)
    return gdf


def rename_earning_info(df: DataFrame, mapping_dict: dict) -> DataFrame:
    """
    Rename the columns of the earning info DataFrame
    - Parameters
        - df: Input DataFrame
        - mapping_dict: Dictionary of columns names
    - Returns
        - df: DataFrame with renamed columns
    """
    # rename the columns
    df = df.rename(columns=mapping_dict)
    return df


def rename_population_info(df: DataFrame, mapping_dict: dict) -> DataFrame:
    """
    Rename the columns of the population info DataFrame
    - Parameters
        - df: Input DataFrame
        - mapping_dict: Dictionary of columns names
    - Returns
        - df: DataFrame with renamed columns
    """

    # drop the columns we don't need
    columns_to_drop = ["S/T code", "GCCSA code", "GCCSA name", "SA4 code", "SA4 name", "SA3 code", "SA3 name"]
    df = df.drop(columns_to_drop, axis=1)
    # rename the columns
    df = df.rename(columns=mapping_dict)
    return df


def rename_corresponding_sa2_to_postcode(df: DataFrame, mapping_dict: dict) -> DataFrame:
    """
    Rename the columns of the corresponding sa2 to postcode DataFrame
    - Parameters
        - df: Input DataFrame
        - mapping_dict: Dictionary of columns names
    - Returns
        - df: DataFrame with renamed columns
    """
    # drop the columns we don't need
    columns_to_drop = ["POSTCODE.1", "PERCENTAGE"]
    df = df.drop(columns_to_drop, axis=1)
    # rename the columns
    df = df.rename(columns=mapping_dict)
    return df


def rename_postcode_to_sa2_columns(df: DataFrame, columns: list[str]) -> DataFrame:
    """
    Rename and standardise datatype for key columns of the postcode to SA2 dataframe.
    - Parameters
        - df: Input dataframe with postcode and sa2 values
        - columns: A list of specific columns to standardise datatype
    - Returns
        - Dataframe with updated columns
    """
    # standardise column casing to lowercase
    df.columns = df.columns.str.lower()

    # remove null/duplicate entries
    df = df.dropna()
    df = df.drop_duplicates()

    # standardise data types
    df[columns] = df[columns].astype(int)

    return df

def rename_sa2_2016_to_sa2_2021_columns(df: DataFrame, columns: list[str]) -> DataFrame:
    """
    Rename and standardise datatype for key columns of the SA2 2016 to 2021 dataframe.
    - Parameters
        - df: Input dataframe with SA2 2016 and SA2 2021 values
        - columns: A list of specific columns to standardise datatype
    - Returns
        - Dataframe with updated columns
    """
    RATIO_FROM_TO = "ratio_from_to"
    AXIS = 1

    # standardise column casing to lowercase
    df.columns = df.columns.str.lower()

    # name change for ratio column
    df = df.rename({RATIO_FROM_TO: RATIO}, axis=AXIS)

    # remove null/duplicate entries
    df = df.dropna()
    df = df.drop_duplicates()

    # standardise data types
    df[columns] = df[columns].astype(int)
    df[RATIO] = df[RATIO].astype(float)

    return df


# Type Casting
def cast_population_info(df: DataFrame) -> DataFrame:
    """
    Type Cast the Population info dataframe
    - Parameters
        - df: Input DataFrame
    - Returns
        - df: DataFrame with casted dataframe
    """
    df[SA2_CODE] = df[SA2_CODE].astype(int)
    df[SA2_CODE] = df[SA2_CODE].astype(str)
    return df


def cast_corresponding_sa2_to_postcode(df: DataFrame) -> DataFrame:
    """
    Type Cast the SA2 to Postcode dataframe
    - Parameters
        - df: Input DataFrame
    - Returns
        - df: DataFrame with casted dataframe
    """
    df[SA2_CODE] = df[SA2_CODE].astype(int)
    df[SA2_CODE] = df[SA2_CODE].astype(str)
    return df


def cast_earning_info(df: DataFrame) -> DataFrame:
    """
    Type Cast the Earning info dataframe
    - Parameters
        - df: Input DataFrame
    - Returns
        - df: DataFrame with casted dataframe
    """
    df[SA2_CODE] = df[SA2_CODE].astype(str)
    return df


def cast_shape_file(df: DataFrame) -> DataFrame:
    """
    Type Cast the SA2 shape dataframe
    - Parameters
        - df: Input DataFrame
    - Returns
        - df: DataFrame with casted dataframe
    """
    df[SA2_CODE] = df[SA2_CODE].astype(str)
    return df


# Data Cleaning
def clean_population_info(df: DataFrame, target: str) -> DataFrame:
    """
    Clean the null value in the DataFrame and save it in curated data folder
    - Parameters
        - df: Input DataFrame
        - target: Target path to save the cleaned DataFrame
    - Returns
        - df: DataFrame with cleaned data
    """

    df = df.dropna(how = "all")
    # drop the last row, which save the total population in Australia not in sa2
    df = df[:-1]
    # Need to filter out some more
    df = df[~df.isna().any(axis=1)]

    # save it to curated data folder
    # df.to_csv(target, index=False)
    return df


def clean_corresponding_sa2_to_postcode(df: DataFrame, target: str) -> DataFrame:
    """
    Clean the null value in the DataFrame and save it in curated data folder
    - Parameters
        - df: Input DataFrame
        - target: Target path to save the cleaned DataFrame
    - Returns
        - df: DataFrame with cleaned data
    """

    df = df.dropna(how = "all")
    # drop the last row, which is not a part of the data
    df = df[:-1]
    # df.to_csv(target, index=False)
    return df


def clean_earning_info(df: DataFrame, target: str) -> DataFrame:
    """
    Clean the null value in the DataFrame and save it in curated data folder
    - Parameters
        - df: Input DataFrame
        - target: Target path to save the cleaned DataFrame
    - Returns
        - df: DataFrame with cleaned data
    """
    df = df.dropna(how = "all")
    df = df.replace("np", 0)
    df = df.replace("ma", 0)
    # This will drop the rows where the sa2_code column is not numeric
    # drop the rows where the sa2_code column is not numeric
    df = df[pd.to_numeric(df["sa2_code"], errors="coerce").notna()]
    # One more check to remove any row where its NaN
    df = df[~df.isna().any(axis=1)]
    return df


def clean_shape_file(gdf: gpd.GeoDataFrame, target: str) -> gpd.GeoDataFrame:
    """
    Clean the rows with all null values in the GeoDataFrame and save it in curated data folder
    - Parameters
        - gdf: Input GeoDataFrame
        - target: Target path to save the cleaned GeoDataFrame
    - Returns
        - gdf: GeoDataFrame with cleaned data
    """

    gdf = gdf.dropna(how = "all")
    # gdf.to_file(target)
    return gdf


def merge_postcode_to_sa2_mappings(spark: SparkSession, left_df: DataFrame, right_df: DataFrame):
    """
    Merge two dataframes, postcode_df and sa2_df, to map a postcode to a Statistical Areas 2
    code from 2016 and 2021. To be relevant in identifying consumers' location.
    - Parameters
        - spark: SparkSession object
        - left_df: Dataframe containing postcode and SA2 code from 2016
        - right_df: Dataframe containing SA2 code from 2016 and 2021
    - Returns
        - merged_df: Dataframe containing postcode, SA2 code from 2016 and 2021
    """
    SA2_MAINCODE_2016 = "sa2_maincode_2016"
    left_cols = [POSTCODE, SA2_MAINCODE_2016]
    right_cols = [SA2_MAINCODE_2016, SA2_CODE_2021, RATIO]
    AXIS = 1

    # filter relevant dataframes by wanted columns
    left_df = left_df[left_cols]
    right_df = right_df[right_cols]
    # perform postcode to 2016 SA2 and 2016 SA2 to 2021 SA2 merging
    merged_df = left_df.merge(right_df, on=SA2_MAINCODE_2016, how=INNER_JOIN)
    merged_df = merged_df.rename({
        SA2_MAINCODE_2016: SA2_CODE_2016
    }, axis=AXIS)

    return merged_df
