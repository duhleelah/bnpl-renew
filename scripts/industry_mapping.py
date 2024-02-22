from .constants import *
from .misc_changes import *
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

START_INDUS = 'A'

def mapping_industry(sdf: DataFrame) -> DataFrame:
    """
    Maps the industry to tags provided to an respecting industry
    - Parameters:
        - sdf: Spark dataframe that includes merchant abn, industry tags, revenue level and takerate
    - Returns:
        - Dataframe that includes merchant abn, revenue level, take rate, industry tags and the mapped industry
    """

    spark = create_spark()
    # reading the dataframe
    tags = spark.read.parquet(sdf) 
    # processing the industry tags column using extract_from_tags
    tags_df = extract_from_tags(tags)
    industry_tags = tags_df.select(INDUSTRY_TAGS)
    industry_tags = industry_tags.distinct()
    # matching the tags to an hypothetical industry
    matching = dict()
    arbatry_indus = START_INDUS
    for row in industry_tags.select(INDUSTRY_TAGS).collect():
        x = row[INDUSTRY_TAGS]
        matching[x] = arbatry_indus
        arbatry_indus = chr(ord(arbatry_indus) + 1)

    mapping_udf = udf(lambda industry: matching.get(industry, industry), StringType())
    # convering to pandas and saving as a csv file
    df_mapped= tags_df.withColumn(MAPPED_INDUSTRY, mapping_udf(tags_df[INDUSTRY_TAGS]))
    df_mapped_pd = df_mapped.toPandas()
    output_path = INDUSTRY_MAPPING_PATH
    df_mapped_pd.to_csv(output_path, index=False)


mapping_industry(TBL_MERCHANTS_PATH)
