from .constants import *
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
import sys

import os
import requests
from zipfile import ZipFile
import pandas as pd
import geopandas as gpd

def download_data(spark: SparkSession, url: str, save_path: str):
    """
    Download the external data from urls and save to the path
    - Parameters
        - spark: SparkSession
        - url: url of the external data
        - save_path: path to save the external data
    - Returns
        - None
    """

    spark = create_spark()

    # confirm the save_path is exist
    os.makedirs(os.path.dirname(save_path), exist_ok=True)
    
    # download the data from url
    response = requests.get(url)
    
    if response.status_code == 200:
        with open(save_path, 'wb') as file:
            file.write(response.content)
        
        # if the file is zip file, unzip it
        if save_path.endswith(ZIP):
            with ZipFile(save_path, 'r') as zip_file:
                zip_file.extractall(os.path.dirname(save_path))
        
        print(f"Download data successfully to {save_path}")
    else:
        print(f"Download data unccessfully to ({response.status_code})")



def extract_data(spark: SparkSession, path_name:str):
    """
    Extract the landing data from the path
    - Parameters
        - spark: SparkSession
        - path_name: path of the landing data
    - Returns
        - output_df: DataFrame of the landing data
    """

    if path_name.endswith('.shp'):
        # read the shape file and return the GeoDataFrame
        output_df = gpd.read_file(path_name)
        print("Extracted SA2 shapefile successfully")
    elif EARNING_INFO_BY_SA2_PATH in path_name:
        # read the excel file, choose the fouth sheet, skip the blank rows and return the DataFrame
        output_df = pd.read_excel(path_name, sheet_name=4, skiprows=6)
        print("Extracted earning info by SA2 successfully")
    elif POPULATION_INFO_BY_AGE_BY_SEX_BY_SA2_PATH in path_name:
        # read the excel file, choose the first and second sheet, skip the blank rows and return the DataFrame
        male_population_df = pd.read_excel(path_name, sheet_name="Table 1", skiprows=7)
        print("Extracted male population info by age by SA2 successfully")
        female_population_df = pd.read_excel(path_name, sheet_name="Table 2", skiprows=7)
        print("Extracted female population info by age by SA2 successfully")
        return male_population_df, female_population_df
    elif CORRESPONDING_SA2_TO_POSTCODE_PATH in path_name:
        # read the excel file, choose the third sheet, skip the blank rows and return the DataFrame
        output_df = pd.read_excel(path_name, sheet_name="Table 3", skiprows=5)
        print("Extracted corresponding SA2 to postcode successfully")
    elif POSTCODE_TO_2016_SA2_PATH in path_name:
        # read the CSV file and return the pandas dataframe
        output_df = pd.read_csv(path_name)
        print("Extracted postcode to SA2 2016 successfully")
    elif SA2_2016_TO_SA2_2021_PATH in path_name:
        # read the CSV file and return the pandas dataframe
        output_df = pd.read_csv(path_name)
        print("Extracted SA2 2016 to SA2 2021 successfully")
    else:
        # if the PATHS_NAME is not exist, raise the ValueError
        raise ValueError("Unknown PATHS_NAME")
    return output_df


