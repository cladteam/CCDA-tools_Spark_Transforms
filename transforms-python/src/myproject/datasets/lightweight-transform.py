import polars as pl
from transforms.api import transform, lightweight, Input, LightweightInput, Output, LightweightOutput


@lightweight
@transform(
#    ccda_cedars_response_files=Input("ri.foundry.main.dataset.119054ed-4719-4d84-99ba-43625bcafd0f"),
#    ccda_medex_response_files=Input("ri.foundry.main.dataset.ca873ab5-748b-4f53-9ae4-0c819c7fa3d4"),
    lightweight_test=Output("/All of Us-cdb223/HIN - HIE/CCDA/lightweight_test"),
    discovered_codes_expanded=Output("/All of Us-cdb223/HIN - HIE/CCDA/discovered_codes_expanded"),
    discovered_codes=Output("/All of Us-cdb223/HIN - HIE/CCDA/discovered_codes"),
    discovered_codes_with_counts=Output("/All of Us-cdb223/HIN - HIE/CCDA/discovered_codes_with_counts")
)
def compute(
    #ccda_cedars_response_files: LightweightInput,
    #ccda_medex_response_files: LightweightInput,
    discovered_codes_expanded: LightweightOutput,
    discovered_codes: LightweightOutput,
    discovered_codes_with_counts: LightweightOutput
):
    df_custom = pl.DataFrame({"phrase": ["Hello", "World"]})
    #df_input = ccda_cedars_response_files.polars().limit(10)
    # Remove comment to write contents of df_custom to discovered_codes_expanded on build
    lightweight_test.write_table(df_custom)
