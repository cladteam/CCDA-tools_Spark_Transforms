# from pyspark.sql import DataFrame
# 
# # from pyspark.sql import functions as F
# from transforms.api import Input, Output, transform_df
# 
# from myproject.datasets import utils
# 
# 
# @transform_df(
#     Output("/All of Us-cdb223/HIN - HIE/TARGET_DATASET_PATH"),
#     source_df=Input("/All of Us-cdb223/HIN - HIE/SOURCE_DATASET_PATH"),
# )
# def compute(source_df: DataFrame) -> DataFrame:
#     return utils.identity(source_df)
