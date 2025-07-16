from transforms.api import transform, Input, Output, incremental, configure
import os
import io
import logging
import re
import lxml.etree as ET
from ..xml_ns import ns
from collections import defaultdict
from pyspark.sql import functions as F
from pyspark.sql import DataFrame
from pyspark.sql.types import StructType, StructField, StringType
from typing import List, Dict

# Configure logging
logging.basicConfig(
    format='%(levelname)s: %(message)s',
    filename="log_dq_snooper_section_spark.log",
    level=logging.WARNING
)
logger = logging.getLogger(__name__)

# Configuration variables - modify these as needed
###INPUT_DATASET_NAME = "ri.foundry.main.dataset.8c8ff8f9-d429-4396-baed-a3de9c945f49"  # Input dataset name in Foundry
####OUTPUT_DATASET_NAME = "dq_ccda_snooper_section_spark"  # Output dataset name 
EXPORT_TO_FOUNDRY = True  # Set to True to export results to Foundry
EXPORT_TO_CSV = False  # Set to True to export results to CSV

# Output schema for Spark DataFrame
section_snooper_schema = StructType([
    StructField("source", StringType(), True),
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
    StructField("path", StringType(), True)
])

# Code exclusion list
code_exclusion_list = [
    '33999-4', '10157-6', '64572001', '33999-4', '48767-8', '282291009', '55607006'
]


def process_xml_file(file_path, xml_string):  # noqa: C901
    """Process a single XML file and extract records from it"""
    records = []

    root = ET.fromstring(xml_string)
    tree = ET.ElementTree(root)

    # Find all 'section' elements in the XML tree using the specified namespace
    section_elements = tree.findall(".//section", ns)

    for section_element in section_elements:
        # Default section template ID if not found
        section_template_id = "n/a"

        # Extract templateId from the section (if available)
        section_template_ele = section_element.findall("templateId", ns)
        if len(section_template_ele) > 0:
            section_template_id = section_template_ele[0].get('root')

        # Extract section code and display name
        section_code_elems = section_element.findall("code", ns)
        if not section_code_elems:
            continue

        section_code = section_code_elems[0].get('code')
        section_name = section_code_elems[0].get('displayName')

        # Find all 'entry' elements within the section
        entry_elements = section_element.findall("entry", ns)

        for entry_ele in entry_elements:
            # Dictionary to store extracted values, grouped by XML path
            value_dict = defaultdict(list)
            code_dict = defaultdict(list)

            # Extract all 'value' elements within the entry
            value_elements = entry_ele.findall(".//value", ns)
            for value_ele in value_elements:
                # Clean the XML path (remove namespace references)
                value_path = re.sub(r'{.*?}', '', tree.getelementpath(value_ele))
                value_path = "/".join(value_path.split("/")[:-1])  # Get parent path

                value_attribs_dict = {}
                for (attr, value) in value_ele.attrib.items():
                    clean_attr = re.sub(r'{.*}', '', attr)
                    clean_value = re.sub(r'{.*}', '', value)
                    value_attribs_dict[clean_attr] = clean_value

                # Dict of (attribute-dict, text-value) pairs, keyed by path
                value_dict[value_path].append((value_attribs_dict, value_ele.text))

            # Extract all 'code' elements within the entry
            code_elements = entry_ele.findall(".//code", ns)
            for code_ele in code_elements:
                # Clean the XML path (remove namespace references)
                code_path = re.sub(r'{.*?}', '', tree.getelementpath(code_ele))
                code_path = "/".join(code_path.split("/")[:-1])  # Get parent path

                code_attribs_dict = {}
                for (attr, code) in code_ele.attrib.items():
                    clean_attr = re.sub(r'{.*}', '', attr)
                    clean_code = re.sub(r'{.*}', '', code)
                    code_attribs_dict[clean_attr] = clean_code

                # Dict of (attribute-dict, text-value) pairs, keyed by path
                code_dict[code_path].append((code_attribs_dict, code_ele.text))

                code_value_dict = defaultdict(list)

                # Merge code_dict and value_dict
                for d in (code_dict, value_dict):
                    for key, value in d.items():
                        code_value_dict[key].extend(value)  # Preserve list values

                # Retrieve corresponding value(s) for the code (if any)
                # Tuple contains (attributes dictionary, text content)
                # BUT NOT COMPLETELY!!! just for this one code_path????
                code_value_tuple_list = code_value_dict[code_path]
                code_value_tuple_list = [t for t in code_value_tuple_list if 'nullFlavor' not in t[0]]

                if code_ele.get('code', '') in code_exclusion_list:
                    continue

                for code_value_tuple in code_value_tuple_list:
                    # Construct a new row for the DataFrame
                    record = {
                        'source': os.path.basename(file_path),
                        'section': section_template_id,
                        'section_code': section_code,
                        'section_name': section_name,
                        'path': code_path,
                        # code_ele?
                        'code': code_ele.get('code', ''),
                        'codeSystem': code_ele.get('codeSystem', ''),
                        # values
                        'value_type': code_value_tuple[0].get('type', ''),
                        'value_unit': code_value_tuple[0].get('unit', ''),
                        'value_value': code_value_tuple[0].get('value', ''),
                        'value_code': code_value_tuple[0].get('code', ''),
                        'value_codeSystem': code_value_tuple[0].get('codeSystem', ''),
                        'value_text': code_value_tuple[1].strip() if code_value_tuple[1] else ''
                    }
                    records.append(record)

    return records


def get_file_paths_from_dataframe(df: DataFrame, file_column: str = "filePath") -> List[str]:
    """Extract file paths from a DataFrame column"""
    # Collect file paths as local list
    file_paths = [row[file_column] for row in df.select(file_column).collect()]
    return file_paths


@configure(profile=['DRIVER_MEMORY_LARGE', 'NUM_EXECUTORS_64'])
@transform(
snooper_section=Output("/All of Us-cdb223/HIN - HIE/CCDA/IdentifiedData/CCDA_spark/dq_ccda_snooper_section"),
    xml_files=Input("ri.foundry.main.dataset.8c8ff8f9-d429-4396-baed-a3de9c945f49"), # 40.9 m rows, 55949 distinct files *OK*
    # xml_files=Input("ri.foundry.main.dataset.119054ed-4719-4d84-99ba-43625bcafd0f"), # 13.77 m rows, 7153 distinct filenames
    # xml_files=Input("ri.foundry.main.dataset.8c8ff8f9-d429-4396-baed-a3de9c945f49"),
)
def compute(snooper_section, xml_files):  # noqa: C901

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
                for thing in process_xml_file(file_status.path, xml_content):
                    yield thing

    files_df = xml_files.filesystem().files('**/*.xml')
    rdd = files_df.rdd.flatMap(process_file)
    processed_df = rdd.toDF(section_snooper_schema)
    snooper_section.write_dataframe(processed_df)