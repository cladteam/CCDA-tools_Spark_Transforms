from transforms.api import transform, Input, Output, configure
from pyspark.sql import types as T
import os
import io
import re
import lxml.etree as ET
#from xml_ns import ns
from collections import defaultdict
from ..xml_ns import ns

"""
    Derived from the dq_ccda_snooper_section_simple, which in turn is
    dervied from the dq_ccda_snooper_section. 
"""

section_snooper_schema =  T.StructType([
    T.StructField("source", T.StringType(), True),
    T.StructField("section", T.StringType(), True),
    T.StructField("section_code", T.StringType(), True),
    T.StructField("section_name", T.StringType(), True),
    T.StructField("codeSystem", T.StringType(), True),
    T.StructField("code", T.StringType(), True),
    T.StructField("value_type", T.StringType(), True),
    T.StructField("value_unit", T.StringType(), True),
    T.StructField("value_value", T.StringType(), True),
    T.StructField("value_code", T.StringType(), True),
    T.StructField("value_codeSystem", T.StringType(), True),
    T.StructField("value_text", T.StringType(), True),
    T.StructField("path", T.StringType(), True)
])

#todo, convert to exclude list from dataset
code_exclusion_list = [ '33999-4' ]


def snoop_for_tag(starting_ele, tag, tree):
    """
        finds all tagged elements below the passed-in starting_ele

        parameter: tag is just the simple name of the tag like code or value
        returns:  Dict of (attribute-dict, text-value) pairs, keyed by path
        returns:  Dict  path -> (attribute-dict, text-value) 
        returns:  {path:( {attr: attr-value}, text-value)  }
    """
    value_dict = {}
    elements = starting_ele.findall(f".//{tag}", ns)
    for value_ele in elements:
        # Clean the XML path (remove namespace references)
        value_path = re.sub(r'{.*?}', '', tree.getelementpath(value_ele))
        value_path = "/".join(value_path.split("/")[:-1])  # Get parent path

        value_attribs_dict = defaultdict(str)
        for (attr, value) in value_ele.attrib.items():
            clean_attr = re.sub(r'{.*}', '', attr)
            clean_value = re.sub(r'{.*}', '', value)
            value_attribs_dict[clean_attr] = clean_value

        if value_path in value_dict:
            value_dict[value_path].append((value_attribs_dict, value_ele.text))
        else:
            value_dict[value_path] = (value_attribs_dict, value_ele.text)

    return value_dict


def snoop_sections(tree, file_path):
    records = []

    section_elements = tree.findall(".//section", ns)
    for section_element in section_elements:

        section_template_id = "n/a"
        section_template_ele = section_element.findall("templateId", ns)
        if len(section_template_ele) > 0:
            section_template_id = section_template_ele[0].get('root')

        section_code = section_element.findall("code", ns)[0].get('code')
        section_name = section_element.findall("code", ns)[0].get('displayName')

        entry_elements = section_element.findall("entry", ns)
        for entry_ele in entry_elements:

            #   {path:( {attr: attr-value}, text-value)  }
            code_dict = snoop_for_tag(entry_ele, "code", tree)
            value_dict = snoop_for_tag(entry_ele, "value", tree)
            for code_path in code_dict:
                record = {
                    'source'      : os.path.basename(file_path),
                    'section'     : section_template_id,
                    'section_code': section_code,
                    'section_name': section_name,
                    'path'        : code_path,

                    'code'        : code_dict[code_path][0]['code'],
                    'codeSystem'  : code_dict[code_path][0]['codeSystem'],

                    'value_type'  : None,
                    'value_unit'  : None,
                    'value_value' : None,
                    'value_code'  : None,
                    'value_codeSystem': None,

                    'value_text'  : ( code_dict[code_path][1].strip() if code_dict[code_path][1] else None )
                }
                if code_path in value_dict:
                    # is this ever true?
                    record['value_type'] = value_dict[code_path][0]['type']
                    record['value_unit'] = value_dict[code_path][0]['unit']
                    record['value_value']= value_dict[code_path][0]['value']
                    record['value_code'] = value_dict[code_path][0]['code']
                    record['value_codeSystem'] = value_dict[code_path][0]['codeSystem']
                
                records.append(record)

            for code_path in (value_dict.keys() - code_dict.keys()):
                record = {
                    'source'      : os.path.basename(file_path),
                    'section'     : section_template_id,
                    'section_code': section_code,
                    'section_name': section_name,
                    'path'        : code_path,

                    'code'        : None,
                    'codeSystem'  : None,

                    'value_type'  : value_dict[code_path][0]['type'],
                    'value_unit'  : value_dict[code_path][0]['unit'],
                    'value_value' : value_dict[code_path][0]['value'],
                    'value_code'  : value_dict[code_path][0]['code'],
                    'value_codeSystem': value_dict[code_path][0]['codeSystem'],

                    'value_text'  : None,
                }
                records.append(record)

    return records


def parse_string(file_path, xml_string):
    root = ET.fromstring(xml_string)
    tree = ET.ElementTree(root)
    return snoop_sections(tree, file_path)


@configure(profile=['DRIVER_MEMORY_LARGE', 'NUM_EXECUTORS_16' ])
@transform(
    snooper_section = Output("/All of Us-cdb223/HIN - HIE/CCDA/IdentifiedData/CCDA_spark/dq_ccda_snooper_section"),
    xml_files=Input("ri.foundry.main.dataset.8c8ff8f9-d429-4396-baed-a3de9c945f49")
)
def compute(snooper_section, xml_files):

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
                yield(parse_string(file_status.path, xml_content))

### NOTE THE LIMIT!!
    files_df = xml_files.filesystem().files('**/*.xml').limit(10)
    rdd = files_df.rdd.flatMap(process_file)
    processed_df = rdd.toDF(section_snooper_schema)
    snooper_section.write_dataframe(processed_df)
