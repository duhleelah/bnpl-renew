from .constants import *
from .external_extract import *
from pyspark.sql import DataFrame
import os
import shutil

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
        if os.path.exists(target):
            if os.path.isdir(target):
                # Remove the directory
                shutil.rmtree(target)
                print("Removed Target")
            else:
                # Remove the file
                os.remove(target)
                print("Removed Target")
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
