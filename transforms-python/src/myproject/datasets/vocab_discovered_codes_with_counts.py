from transforms.api import transform_df, Input, Output
from pyspark.sql import functions as F


@transform_df(
    Output("ri.foundry.main.dataset.0e0173a9-6b1d-43b3-96c9-7440816845b4"),
    discovered_codes_expanded=Input(
        "ri.foundry.main.dataset.160c32e8-774e-4e71-b137-766cfc2bdccc"
    ),
    codemap_xwalk=Input("ri.foundry.main.dataset.28fe6af8-0b22-4b45-86e2-b394c62dcd09"),
)
def compute(ctx, discovered_codes_expanded, codemap_xwalk):
    # Group by codeSystem and src_cd to get counts
    grouped_df = (
        discovered_codes_expanded.groupBy(["codeSystem", "src_cd"])
        .count()
        .select(
            F.col("codeSystem"),
            F.col("src_cd"),
            F.col("count").alias("counts"),
        )
    )

    # Create a mapping table that checks if a code exists in the codemap_xwalk
    # Using groupBy to deduplicate and get a single is_match flag per combination
    code_exists_map = (
        codemap_xwalk.filter(
            (F.col("src_vocab_code_system").isNotNull())
            & (F.col("src_code").isNotNull())
            & (F.col("target_concept_id").isNotNull())
            & (F.col("target_concept_id") > 1)
        )
        .groupBy("src_vocab_code_system", "src_code")
        .agg(F.lit(1).alias("is_match"))
    )

    # Perform a left join to check if codes exist
    result_df = grouped_df.join(
        F.broadcast(code_exists_map),
        (grouped_df.codeSystem == code_exists_map.src_vocab_code_system)
        & (grouped_df.src_cd == code_exists_map.src_code),
        "left_outer",
    )

    # Fill null values with 0 for is_match
    final_df = result_df.select(
        F.col("src_cd"),
        F.col("codeSystem"),
        F.col("counts"),
        F.coalesce(F.col("is_match"), F.lit(0)).alias("is_match"),
    )

    return final_df


@transform_df(
    Output(
        "/All of Us-cdb223/HIN - HIE/CCDA/transform/vocab_discovered_codes_with_counts_and_resource"
    ),
    discovered_codes_expanded=Input(
        "ri.foundry.main.dataset.160c32e8-774e-4e71-b137-766cfc2bdccc"
    ),
)
def compute_with_code_system_name(ctx, discovered_codes_expanded):
    """
    This transform groups discovered codes by codeSystem, src_cd, and resource,
    counting occurrences and returning a dataset with code system information.
    """
    return (
        discovered_codes_expanded.groupBy(["codeSystem", "src_cd", "resource"])
        .count()
        .select(
            F.col("resource").alias("codeSystemName"),
            F.col("codeSystem"),
            F.col("src_cd").alias("sourceCode"),
            F.col("count").alias("occurrenceCount"),
        )
    )
