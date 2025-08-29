# from pyspark.sql import functions as F
from transforms.api import transform_df, Input, Output


@transform_df(
    Output("/All of Us-cdb223/HIN - HIE/CCDA/transform/vocab_discovered_codes_union"),
    codes=Input("/All of Us-cdb223/HIN - HIE/CCDA/transform/vocab_discovered_codes"),
    codes_test=Input("/All of Us-cdb223/HIN - HIE/CCDA/transform/vocab_discovered_codes_test_files")
)
def compute(codes, codes_test):
    return codes.union(codes_test).distinct()
