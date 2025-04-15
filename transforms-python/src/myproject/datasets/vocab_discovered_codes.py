# from pyspark.sql import functions as F
from transforms.api import transform_df, Input, Output
from pyspark.sql import types as  T

@transform_df(
    Output("ri.foundry.main.dataset.e5fc74fe-6b25-4de0-b722-088575f62ed9"),
    discovered_codes_expanded = Input("ri.foundry.main.dataset.160c32e8-774e-4e71-b137-766cfc2bdccc"),
)
def compute(ctx, discovered_codes_expanded):
    return(discovered_codes_expanded.select(['codeSystem', 'src_cd']).distinct())
