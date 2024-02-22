from .constants import *
from .external_extract import *
from .external_transform import *
from .external_load import *
from .read import *
from .join import *

def get_external_landing_files(spark: SparkSession, prefix:str="") -> None:
    """
    Get the landing files of the external datasets
    - Parameters
        - spark: SparkSession to save the files
        - prefix: Prefix so function can run from different directories
    - Returns
        - None
    """
    print("Downloading...")
    download_data(spark, AU_SA2_SHAPE_FILE_URL, prefix + ZIP_SA2_SHAPE_FILE_PATH)
    download_data(spark, EARNING_INFO_BY_SA2_URL, prefix + EARNING_INFO_BY_SA2_PATH)
    download_data(spark, POPULATION_INFO_BY_AGE_BY_SEX_BY_SA2_URL, prefix + POPULATION_INFO_BY_AGE_BY_SEX_BY_SA2_PATH)
    download_data(spark, CORRESPONDING_SA2_TO_POSTCODE_URL, prefix + ZIP_CORRESPONDING_SA2_TO_POSTCODE_PATH)
    download_data(spark, POSTCODE_TO_2016_SA2_URL, prefix + POSTCODE_TO_2016_SA2_PATH)
    download_data(spark, SA2_2016_TO_SA2_2021_URL, prefix + SA2_2016_TO_SA2_2021_PATH)
    print("FINISHED Downloading")
    return

def external_raw_files(spark:SparkSession, prefix:str="") -> None:
    """
    Get the raw files of the external datasets
    - Parameters
        - spark: SparkSession to save the files
        - prefix: Prefix so function can run from different directories
    - Returns
        - None
    """
    print("Extracting...")
    sa2_shape_file = extract_data(spark, prefix + SA2_SHAPE_FILE_PATH)
    earning_info_by_sa2 = extract_data(spark, prefix + EARNING_INFO_BY_SA2_PATH)
    male_population_info_by_age_by_sa2, female_population_info_by_age_by_sa2 = extract_data(spark, prefix + POPULATION_INFO_BY_AGE_BY_SEX_BY_SA2_PATH)
    corresponding_sa2_to_postcode = extract_data(spark, prefix + CORRESPONDING_SA2_TO_POSTCODE_PATH)
    postcode_to_2016_sa2 = extract_data(spark, prefix + POSTCODE_TO_2016_SA2_PATH)
    sa2_2016_to_sa2_2021 = extract_data(spark, prefix + SA2_2016_TO_SA2_2021_PATH)
    print("FINISHED Extracting")

    # Rename columns
    sa2_shape_file = rename_shape_file(sa2_shape_file, SHAPE_COLS_DICT)
    earning_info_by_sa2 = rename_earning_info(earning_info_by_sa2, EARNING_INFO_COLS_DICT)
    male_population_info_by_age_by_sa2 = rename_population_info(male_population_info_by_age_by_sa2, MALE_POPULATION_COLS_DICT)
    female_population_info_by_age_by_sa2 = rename_population_info(female_population_info_by_age_by_sa2, FEMALE_POPULATION_COLS_DICT)
    corresponding_sa2_to_postcode = rename_corresponding_sa2_to_postcode(corresponding_sa2_to_postcode, CORRESPONDING_SA2_TO_POSTCODE_COLS_DICT)
    postcode_to_2016_sa2 = rename_postcode_to_sa2_columns(postcode_to_2016_sa2, columns=[POSTCODE, SA2_MAINCODE_2016])
    sa2_2016_to_sa2_2021 = rename_sa2_2016_to_sa2_2021_columns(sa2_2016_to_sa2_2021, columns=[SA2_MAINCODE_2016, SA2_CODE_2021])

    print("Loading...")
    load_external_shape_file(SHP, sa2_shape_file, prefix + RAW_SA2_SHAPE_FILE_PATH)
    load_external_data(CSV, earning_info_by_sa2, prefix + RAW_EARNING_INFO_BY_SA2_PATH)
    load_external_data(CSV, male_population_info_by_age_by_sa2, prefix + RAW_MALE_POPULATION_INFO_BY_AGE_BY_SEX_BY_SA2)
    load_external_data(CSV, female_population_info_by_age_by_sa2, prefix + RAW_FEMALE_POPULATION_INFO_BY_AGE_BY_SEX_BY_SA2)
    load_external_data(CSV, corresponding_sa2_to_postcode, prefix + RAW_CORRESPONDING_SA2_TO_POSTCODE_PATH)
    load_external_data(CSV, postcode_to_2016_sa2, prefix + RAW_POSTCODE_TO_2016_SA2_PATH)
    load_external_data(CSV, sa2_2016_to_sa2_2021, prefix + RAW_SA2_2016_TO_SA2_2021_PATH)
    print("FINISHED Loading")
    return

def external_transform_files(spark:SparkSession, prefix:str="") -> None:
    """
    Transform the raw files INDIVIDUALLY, save to curated folder
    - Parameters
        - spark: SparkSession to save the files
        - prefix: Prefix so function can run from different directories
    - Returns
        - None
    """
    print("Transforming...")
    # First get the raw datasets
    sa2_shape_file = read_raw_sa2_shape_file(prefix)
    earning_info_by_sa2 = read_raw_earning_info(prefix)
    male_population_info_by_age_by_sa2, female_population_info_by_age_by_sa2 = read_raw_population_info_age_sex(prefix)
    corresponding_sa2_to_postcode = read_raw_sa2_to_postcodes(prefix)
    postcode_to_2016_sa2 = read_raw_postcodes_to_sa2_2016(prefix)
    sa2_2016_to_sa2_2021 = read_raw_sa2_2016_to_sa2_2021(prefix)

    # Now do the transformations
    sa2_shape_file = clean_shape_file(sa2_shape_file, prefix + CURATED_SA2_SHAPE_FILE_PATH)
    earning_info_by_sa2 = clean_earning_info(earning_info_by_sa2, prefix + CURATED_EARNING_INFO_BY_SA2_PATH)
    male_population_info_by_age_by_sa2 = clean_population_info(male_population_info_by_age_by_sa2, prefix + CURATED_MALE_POPULATION_INFO_BY_AGE_BY_SEX_BY_SA2)
    female_population_info_by_age_by_sa2 = clean_population_info(female_population_info_by_age_by_sa2, prefix + CURATED_FEMALE_POPULATION_INFO_BY_AGE_BY_SEX_BY_SA2)
    corresponding_sa2_to_postcode = clean_corresponding_sa2_to_postcode(corresponding_sa2_to_postcode, prefix + CURATED_CORRESPONDING_SA2_TO_POSTCODE_PATH)
    postcodes_to_sa2 = merge_postcode_to_sa2_mappings(spark, postcode_to_2016_sa2, sa2_2016_to_sa2_2021)

    # Type Casting (CAN'T DO THIS WITH NULLS)
    sa2_shape_file = cast_shape_file(sa2_shape_file)
    earning_info_by_sa2 = cast_earning_info(earning_info_by_sa2)
    male_population_info_by_age_by_sa2 = cast_population_info(male_population_info_by_age_by_sa2)
    female_population_info_by_age_by_sa2 = cast_population_info(female_population_info_by_age_by_sa2)
    # corresponding_sa2_to_postcode = cast_corresponding_sa2_to_postcode(corresponding_sa2_to_postcode)
    external_joined = join_external_data(sa2_shape_file, postcodes_to_sa2,
                                         male_population_info_by_age_by_sa2,
                                         female_population_info_by_age_by_sa2,
                                         earning_info_by_sa2)
    print("FINISHED Transforming")

    # Now save the transformations to curated
    # ONLY WANT TO SAVE JOINED DF
    print("SAVING CURATED FILES...")
    load_external_shape_file(SHP, sa2_shape_file, prefix + CURATED_SA2_SHAPE_FILE_PATH)
    load_external_data(CSV, earning_info_by_sa2, prefix + CURATED_EARNING_INFO_BY_SA2_PATH)
    load_external_data(CSV, male_population_info_by_age_by_sa2, prefix + CURATED_MALE_POPULATION_INFO_BY_AGE_BY_SEX_BY_SA2)
    load_external_data(CSV, female_population_info_by_age_by_sa2, prefix + CURATED_FEMALE_POPULATION_INFO_BY_AGE_BY_SEX_BY_SA2)
    load_external_data(CSV, corresponding_sa2_to_postcode, prefix + CURATED_CORRESPONDING_SA2_TO_POSTCODE_PATH)
    # Can now save the merged dataset
    load_external_data(CSV, external_joined, prefix + CURATED_EXTERNAL_JOINED_DATA)
    print("FINISHED SAVING CURATED FILES")

def run_external_etl(prefix:str=""):
    """
    Run the entire ETL process for the external data
    - Parameters
        - prefix: Prefix so function can run from different directories
    - Returns
        - None
    """
    
    spark = create_spark()

    ### Download ###
    get_external_landing_files(spark, prefix)

    ### Extraction (Save Raw Files)###
    external_raw_files(spark, prefix)

    ### Transformation (Save Individual Files to Curated) ###
    external_transform_files(spark, prefix)

if __name__ == "__main__":
    run_external_etl()