from .constants import *
from .external_extract import *
from pyspark.sql import DataFrame
import os

def load_external_data(type: str, df: DataFrame, target: str) -> None:
    """
    Function is primarily used to save data files in either the raw or curated data folders
    - Parameters
        - type: Input Storage type
        - df: Input Dataframe
        - target: Input file path for storage
    - Returns
        - None
    """

    if type == CSV:
        df.to_csv(target, index=False)

def load_external_shape_file(type: str, gdf: gpd.GeoDataFrame, target_path: str) -> None:
    """
    Function is primarily used to save shape file data in either the raw or curated data folders
    - Parameters
        - type: Input Storage type
        - gdf: Input GeoDataFrame
        - target_path: Input file path for storage
    - Returns
        - None
    """

    if type == SHP:
        # save the shape file 
        gdf.to_file(target_path)
