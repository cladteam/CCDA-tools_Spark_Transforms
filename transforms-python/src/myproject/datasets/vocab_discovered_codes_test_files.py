# from pyspark.sql import functions as F
from transforms.api import transform_df, Input, Output
from pyspark.sql import types as  T

@transform_df(
    Output("/All of Us-cdb223/HIN - HIE/CCDA/transform/vocab_discovered_codes_test_files"),
    discovered_codes_expanded = Input("/All of Us-cdb223/HIN - HIE/CCDA/transform/vocab_discovered_codes_expanded_test_files"),
)
def compute(ctx, discovered_codes_expanded):
    return(discovered_codes_expanded.select(['codeSystem', 'src_cd']).distinct())
