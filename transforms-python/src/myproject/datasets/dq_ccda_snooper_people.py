from transforms.api import transform, Input, Output, configure
from pyspark.sql import types as T
import io
import re
from lxml import etree as ET
from ..xml_ns import ns
import os

people_snooper_schema = T.StructType([
    T.StructField("Filename", T.StringType(), True),
    T.StructField("Gender", T.StringType(), True),
    T.StructField("Race", T.StringType(), True),
    T.StructField("Ethnicity", T.StringType(), True),
    T.StructField("BirthTime", T.StringType(), True)
])


def parse_string(file_path, xml_string):
    """ Parses a document for these attributes.
        ** Assumes ** they only appear once, retrieves the only first
        because it's using find(), not findall()
    """
    root = ET.fromstring(xml_string)
    tree = ET.ElementTree(root)

    elements = [
        ('administrativeGenderCode', 'Gender'),
        ('raceCode', 'Race'),
        ('ethnicGroupCode', 'Ethnicity'),
        ('birthTime', 'BirthTime')
    ]

    # just one record, loop over the fields
    record = {'Filename': os.path.basename(file_path)}
    for tag, label in elements:
        element = tree.find(f'.//hl7:{tag}', namespaces=ns)

        if tag == 'birthTime' and element is not None:
            record[label] = element.get('value')
        elif element is not None:
            record[label] = element.get('displayName')
        else:
            record[label] = 'Unknown'

    return record


@configure(profile=['DRIVER_MEMORY_LARGE', 'NUM_EXECUTORS_64'])
@transform(
    snooper_people=Output("ri.foundry.main.dataset.7062bd72-cbff-425f-ba8f-c03d892f58d9"),
    xml_files=Input("ri.foundry.main.dataset.8c8ff8f9-d429-4396-baed-a3de9c945f49"),
    metadata=Input("/All of Us-cdb223/HIN - HIE/sharedResources/FullyIdentiifed/ccda/ccda_response_metadata"),
    hcs_to_dp=Input("/All of Us-cdb223/HIN - HIE/sharedResources/health_care_site_to_data_partner_id")
)
def compute(snooper_people, xml_files, metadata, hcs_to_dp):
    doc_regex = re.compile(r'(<ClinicalDocument.*?</ClinicalDocument>)', re.DOTALL)
    fs = xml_files.filesystem()

    def process_file(file_status):
        with fs.open(file_status.path, 'rb') as f:
            br = io.BufferedReader(f)
            tw = io.TextIOWrapper(br) 
            contents = tw.readline()
            for line in tw:
                contents += line
            # Basically selecting content between ClincalDocument tags, looping in case > 1
            for match in doc_regex.finditer(contents):
                match_tuple = match.groups(0)
                xml_content = match_tuple[0]
                yield (parse_string(file_status.path, xml_content))

    files_df = xml_files.filesystem().files('**/*.xml')
    rdd = files_df.rdd.flatMap(process_file)
    processed_df = rdd.toDF(people_snooper_schema)

    # First convert inputs to DataFrames
    metadata_df = metadata.dataframe()
    hcs_to_dp_df = hcs_to_dp.dataframe()

    # Select only necessary columns from metadata table for the join
    metadata_df = metadata_df.select("response_file_path", "healthcare_site")

    # Join with metadata to get the healthcare_site
    joined_df = processed_df.join(
        metadata_df,
        processed_df["Filename"] == metadata_df["response_file_path"],
        "left"
    )

    # Join with healthcare_site_to_data_partner_id to get the partner_id
    hcs_to_dp_df = hcs_to_dp_df.select("healthcare_site", "data_partner_id")

    # Final join to get partner_id
    final_df = joined_df.join(
        hcs_to_dp_df,
        joined_df["healthcare_site"] == hcs_to_dp_df["healthcare_site"],
        "left"
    ).select(
        processed_df["*"],  # All columns from processed_df
        hcs_to_dp_df["data_partner_id"].alias("partner_id")  # Add partner_id column
    )

    # Write the result
    snooper_people.write_dataframe(final_df)
