

'''
    Similar to the section snooper, this module creates counts for paths found in the header.
    Very different though, it is not driven by finding code elements. It isn't as open and is
    driven by paths for the different OMOP domains. Ideally these are relative paths that allow
    us to see that data where ever it may exist in the header.

    - Location
    - Care Site
    - Provider

'''

from transforms.api import transform, Input, Output, configure
import os
import io
import logging
import re
import lxml.etree as ET
from ..xml_ns import ns
from collections import defaultdict
from pyspark.sql import DataFrame
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from typing import List
from pyspark.sql import functions as F
from myproject.util import clean_path, keep_path, DOC_TYPE_MAP

logging.basicConfig(
    format='%(levelname)s: %(message)s',
    filename="log_dq_snooper_section_spark.log",
    level=logging.WARNING
)
logger = logging.getLogger(__name__)


OMOP_domain_to_CCDA_path = {
    # componentOf, healthCareFacility, recordTarget, patientRole, location

    # cO/eE/l/hCF/location/addr,  rT/pR/addr
    'Location': ['.//addr'],

    # cO/eE/l/hCF,
    'Care_Site': ['.//healthCareFacility'],

    'Provider' : ['.//assignedEntity', './/assignedPerson'],

    # rT/pR
    'Person' : ['.//patientRole', './/patient'],

    # Co/eE
   'Visit': ['.//encompassingEncounter']
}

def find_elements(path, pseudo_domain, tree, file_path, doc_type_name):
    ''' encompassingEncounter, a special case hammered into the sections here.
        No template ID, will use "0.0.1" 
        Mostly only expect one of these, but returning a list of records.
        "fake" because these come from the header, not the body, and they're not sections
    '''
    records = []
    fake_section_code = "n/a"
    fake_section_template_id = "0.0.1"
    fake_section_name = "header"

    elements = tree.findall(path, ns)

    for ele in elements:
        ele_path = tree.getelementpath(ele)
        ele_path = re.sub(r"{urn:hl7-org:v3}", '', ele_path)
        clean_ele_path = clean_path(ele_path)

        # Don't look in the document body, meaning look in the header.
        if not re.match(r"^component/structuredBody", clean_ele_path):

            code=None
            codeSystem=None
            code_ele = ele.find("code", ns)
            translation_code_ele = ele.find("translation", ns)
            if code_ele is not None:
                code=code_ele.get("code")
                codeSystem=code_ele.get('codeSystem')
            elif translation_code_ele is not None:
                code=translation_code_ele.get("code")
                codeSystem=translation_code_ele.get('codeSystem')
            else:
                code=0
                codeSystem="n/a"

            record = {
                'source': os.path.basename(file_path),
                'document_type': doc_type_name,
                'section': fake_section_template_id,
                'section_code': fake_section_code,
                'section_name': fake_section_name,
                'path': ele_path,
                'clean_path': clean_ele_path,
                'code': code,
                'codeSystem': codeSystem,
                # values
                'value_type': '',
                'value_unit': '',
                'value_value': '',
                'value_code': '',
                'value_codeSystem': '',
                'value_text': '',
                'pseudo_domain': pseudo_domain
            }
            records.append(record)
    return records


section_snooper_schema = StructType([
    StructField("source", StringType(), True),
    StructField("document_type", StringType(), True), 
    StructField("section", StringType(), True),
    StructField("section_code", StringType(), True),
    StructField("section_name", StringType(), True),
    
    StructField("codeSystem", StringType(), True),
    StructField("code", StringType(), True),

    StructField("value_type", StringType(), True),
    StructField("value_unit", StringType(), True),
    StructField("value_value", StringType(), True),
    StructField("value_code", StringType(), True),
    StructField("value_codeSystem", StringType(), True),
    StructField("value_text", StringType(), True),

    StructField("path", StringType(), True),  # indexed
    StructField("clean_path", StringType(), True),
    StructField("pseudo_domain", StringType(), True),
    StructField("partner_id", StringType(), True)
])


code_exclusion_list = [ 
    '33999-4', '10157-6',  '48767-8'
    # see #418 for why we should keep these '64572001',  '282291009', '55607006'
]

path_exclusion_list = [ # not parent paths
    r'.*/section/entry/organizer/code$',
    
    # excluded for the purpose of concept-code snooping, may still want this later
    # In fact, this is redundant because the values in an observationRange don't
    # have corresponding codes that drive the code here. So they would be ignored.
    # FWIW, these values are often very long strings of spaces.
    r'.*/referenceRange/observationRange/code$' 
]


def find_doctype(tree):
    doc_type_description = "" 
    doc_template_elements = tree.findall("./templateId", ns)
    for ele in doc_template_elements:
        root_oid = ele.get('root')
        description = DOC_TYPE_MAP.get(root_oid)
        if description:
            doc_type_description = description
            break

    return doc_type_description


def process_xml_file(file_path, xml_string, verbose=False):  # noqa: C901
    """Process a single XML file and extract records from it"""
    root = ET.fromstring(xml_string)
    tree = ET.ElementTree(root)

    doc_type_name = find_doctype(tree)
    ele_records = []
    for pseudo_domain, path_list in OMOP_domain_to_CCDA_path.items():
        for path in path_list:
            extra_records = find_elements(path, pseudo_domain, tree, file_path, doc_type_name)
            ele_records.extend(extra_records)
    return ele_records


def get_file_paths_from_dataframe(df: DataFrame, file_column: str = "filePath") -> List[str]:
    """Extract file paths from a DataFrame column"""
    # Collect file paths as local list
    file_paths = [row[file_column] for row in df.select(file_column).collect()]
    return file_paths


@configure(profile=['DRIVER_MEMORY_LARGE', 'NUM_EXECUTORS_64'])
@transform(
snooper_section=Output("/All of Us-cdb223/HIN - HIE/CCDA/IdentifiedData/CCDA_spark/dq_ccda_snooper_header"),
    xml_files=Input("ri.foundry.main.dataset.8c8ff8f9-d429-4396-baed-a3de9c945f49"), # 40.9 m rows, 55949 distinct files *OK*
    metadata=Input("/All of Us-cdb223/HIN - HIE/sharedResources/FullyIdentiifed/ccda/ccda_response_metadata"),
    hcs_to_dp=Input("/All of Us-cdb223/HIN - HIE/sharedResources/health_care_site_to_data_partner_id"),
)
def compute(snooper_section, xml_files, metadata, hcs_to_dp):  # noqa: C901
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
                stuff =  process_xml_file(file_status.path, xml_content)
                if stuff:
                    for thing in stuff:
                        yield thing

    files_df = xml_files.filesystem().files('**/*.xml')
    rdd = files_df.rdd.flatMap(process_file)
    processed_df = rdd.toDF(section_snooper_schema)

    # Extract just the filename from the source column for joining
    processed_df = processed_df.withColumn("source_filename", F.col("source")) 

    # First convert inputs to DataFrames
    metadata_df = metadata.dataframe()
    hcs_to_dp_df = hcs_to_dp.dataframe()

    # Select only necessary columns from metadata table for the join
    metadata_df = metadata_df.select("response_file_path", "healthcare_site")

    # Join with metadata to get the healthcare_site
    joined_df = processed_df.join(
        metadata_df,
        processed_df["source_filename"] == metadata_df["response_file_path"],
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

    # Ensure no duplicate columns exist
    final_df = final_df.drop(joined_df["partner_id"])

    # Write the result
    snooper_section.write_dataframe(final_df)
