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
    StructField("date", StringType(), True),  # indexed
    StructField("start_date", StringType(), True),  # indexed
    StructField("end_date", StringType(), True),  # indexed
    StructField("date_debug", StringType(), True),  # indexed
    StructField("start_date_debug", StringType(), True),  # indexed
    StructField("end_date_debug", StringType(), True),  # indexed
    StructField("path", StringType(), True),  # indexed
    StructField("clean_path", StringType(), True),
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


def get_time_value(tree, entry_ele, path, name, depth):
    """ Returns a dictionary from path to a dictoinary of name to date value under effectiveTime.
        path --> name --> value
        The name tells is it is just a value, low or high, but
        uses the names date, start_date, and end_date.
    """
    date_dict = defaultdict(dict)
    for value_ele in entry_ele.findall(path, ns):
        # Clean the XML path (remove namespace references)
        innards = ET.tostring(value_ele, encoding="unicode")
        path = re.sub(r'{.*?}', '', tree.getelementpath(value_ele))
        parent_path = "/".join(path.split("/")[:-depth])  # Get parent path
        value = None
        for attr in value_ele.attrib:
            clean_attr = re.sub(r'{.*?}', '', attr)
            clean_value = re.sub(r'{.*?}', '', value_ele.attrib.get(attr, ''))
            if clean_attr == 'value':
                value = clean_value
        #date_dict[parent_path] = {name: value, (name + '_debug'): f"{value_ele.attrib}" }
        if value:
            date_dict[parent_path] = {name: value, (name + '_debug'): innards }
        else:
            date_dict[parent_path] = {name: "-null-", (name + '_debug'): innards }

    return date_dict 

def collect_time_values(entry_ele, tree):
    # Dict of (name, value) tuples, keyed by path
    date_dict = get_time_value(tree, entry_ele, ".//effectiveTime", "date", 1)
    low_dict = get_time_value(tree, entry_ele, ".//effectiveTime/low", "start_date", 2)
    high_dict  = get_time_value(tree, entry_ele, ".//effectiveTime/high", "end_date", 2)
        
    return date_dict | high_dict | low_dict
    #return date_dict


def collect_value_elements(entry_ele, tree):
    # Dict of (attribute-dict, text-value) pairs, keyed by path
    value_dict = defaultdict(list)
    for value_ele in entry_ele.findall(".//value", ns):
        # Clean the XML path (remove namespace references)
        path = re.sub(r'{.*?}', '', tree.getelementpath(value_ele))
        parent_path = "/".join(path.split("/")[:-1])  # Get parent path
        value_attribs_dict =  { 'type': '', 'unit': '', 'value': '',
                                'code': '', 'codeSystem': ''}
        for attr in value_ele.attrib:
            clean_attr = re.sub(r'{.*?}', '', attr)
            clean_value = re.sub(r'{.*?}', '', value_ele.attrib.get(attr, ''))
            value_attribs_dict[clean_attr] = clean_value
        value_attribs_dict['path'] = parent_path
        value_dict[parent_path].append((value_attribs_dict, value_ele.text))
    return value_dict


def collect_code_elements(entry_ele, tree):
    # Dict of (attribute-dict, text-value) pairs, keyed by path
    code_dict = defaultdict(list)
    for code_ele in entry_ele.findall(".//code", ns):
        code = code_ele.get('code','')
        codeSystem = code_ele.get('codeSystem','')
        if codeSystem != '' and code != '' \
           and code not in code_exclusion_list \
           and 'nullFlavor' not in code_ele.attrib:
            # Clean the XML path (remove namespace references)
            path = re.sub(r'{.*?}', '', tree.getelementpath(code_ele))
            if keep_path(path, path_exclusion_list):
                parent_path = "/".join(path.split("/")[:-1])  # Get parent path
                code_attribs_dict = {}
                for attr in code_ele.attrib:
                    clean_attr = re.sub(r'{.*?}', '', attr)
                    clean_code = re.sub(r'{.*?}', '', code_ele.attrib.get(attr, ''))
                    code_attribs_dict[clean_attr] = clean_code
                code_attribs_dict['path'] = parent_path
                code_dict[parent_path].append((code_attribs_dict, code_ele.text))
    return code_dict
    



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


def find_sections(tree, file_path, doc_type_name, verbose):
    record_dict = {}
    records = []


    section_elements = tree.findall(".//section", ns)
    for section_element in section_elements:
        section_template_id = ""
        section_template_ele = section_element.findall("templateId", ns)
        if len(section_template_ele) > 0:
            section_template_id = section_template_ele[0].get('root')
        section_code_elems = section_element.findall("code", ns)
        if section_code_elems and section_template_id != '':
            section_code = section_code_elems[0].get('code')
            section_name = section_code_elems[0].get('displayName')

            for entry_ele in section_element.findall("entry", ns):
                value_dict = collect_value_elements(entry_ele, tree)
                code_dict = collect_code_elements(entry_ele, tree)
                path_date_dict = collect_time_values(entry_ele, tree)
                for code_path_key in code_dict:
                    for code_tuple in code_dict[code_path_key]:
                        code_row =  code_tuple[0]
                        if len(value_dict[code_path_key]) > 0:
                            for value_tuple in value_dict[code_path_key]: # hope/expect this to be just one
                                value_row =  value_tuple[0]
                                value_text = f"{value_tuple[1].strip() if value_tuple[1] else ''}"  # will convert None to ""
                                record = {
                                    'source': os.path.basename(file_path),
                                    'document_type': doc_type_name, 
                                    'section': section_template_id,
                                    'section_code': section_code,
                                    'section_name': section_name,
                                    'path': code_path_key,
                                    'clean_path': clean_path(code_path_key),
                                    # codes
                                    'code': code_row['code'],
                                    'codeSystem': code_row['codeSystem'],
                                    # values
                                    'value_type': value_row['type'],
                                    'value_unit': value_row['unit'],
                                    'value_value': value_row['value'],
                                    'value_code': value_row['code'],
                                    'value_codeSystem': value_row['codeSystem'],
                                    'value_text': value_text if value_tuple[1] else None,
                                    # dates
                                    'date': None,
                                    'start_date': None,
                                    'end_date': None,
                                    'date_debug': None,
                                    'start_date_debug': None,
                                    'end_date_debug': None,
                                }
                                record_dict[code_path_key]=record
                                if keep_path(clean_path(code_path_key), path_exclusion_list):
                                    if verbose:
                                        print(f"ACCEPTED key:{code_path_key}  type:{type(value_tuple[1])} text:\"{value_tuple[1].strip() if value_tuple[1] else ''}\"  attrs:{value_tuple[0]}   ")
                                        print(f"  {type(record['value_text'])} \"{record['value_text']}\"")
                                    records.append(record)
                                else:
                                    if verbose:
                                        print(f"REJECTED {code_path_key}")
                        else:
                            record = {
                                'source': os.path.basename(file_path),
                                'document_type': doc_type_name,
                                'section': section_template_id,
                                'section_code': section_code,
                                'section_name': section_name,
                                'path': code_path_key,
                                'clean_path': clean_path(code_path_key),
                                # codes
                                'code': code_row['code'],
                                'codeSystem': code_row['codeSystem'],
                                # values
                                'value_type': '',
                                'value_unit': '',
                                'value_value': '',
                                'value_code': '',
                                'value_codeSystem': '',
                                'value_text': '',
                                # dates
                                'date': None,
                                'start_date': None,
                                'end_date': None,
                                'date_debug': None,
                                'start_date_debug': None,
                                'end_date_debug': None,
                            }
                            record_dict[code_path_key]=record
                            if keep_path(clean_path(code_path_key), path_exclusion_list):
                                if verbose:
                                    print(f"ACCEPTED {code_path_key} (no value_* values) ")
                                records.append(record)
                            else:
                                if verbose:
                                    print(f"REJECTED {code_path_key}")
                for path, date_dict in path_date_dict.items():
                    if path in record_dict:
                        for name, value in date_dict.items():
                            record_dict[path][name]=value 

    #return records
    return list(record_dict.values())


def process_xml_file(file_path, xml_string, verbose=False):  # noqa: C901
    """Process a single XML file and extract records from it"""
    root = ET.fromstring(xml_string)
    tree = ET.ElementTree(root)

    doc_type_name = find_doctype(tree)
    section_records = find_sections(tree, file_path, doc_type_name, verbose=False)
    return section_records






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
