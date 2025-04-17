# from pyspark.sql import functions as F
from transforms.api import transform, Input, Output, configure
from pyspark.sql import types as  T
from pyspark.sql import Row
import io
import re
from lxml import etree as ET

discovered_codes_schema =  T.StructType([
    T.StructField("data_source", T.StringType(), True),
    T.StructField("resource", T.StringType(), True),
    T.StructField("data_element_path", T.StringType(), True),
    T.StructField("data_element_node", T.StringType(), True),
    T.StructField("codeSystem", T.StringType(), True),
    T.StructField("src_cd", T.StringType(), True),
    T.StructField("src_cd_description", T.StringType(), True),
    T.StructField("src_cd_unit", T.StringType(), True),
    T.StructField("src_cd_count", T.StringType(), True),
    T.StructField("notes", T.StringType(), True),
    T.StructField("counts", T.StringType(), True)])

def snoop_xml_string(data_source, xml_string):
    """
    for newer spark-based work
    returns a list of dictionary records
    """

    root = ET.fromstring(xml_string)
    tree = ET.ElementTree(root)
    element_list = tree.xpath(".//*[@codeSystem]") 
    ele_count=0

    row_list = []

    for element in element_list:
        ele_count += 1

        element_path = tree.getelementpath(element)
        # Extract attributes, simplify path, remove namespace and conditionals
        data_element_path = re.sub(r'{.*?}', '', element_path)
        data_element_path = re.sub(r'\[.*?\]', '', data_element_path)

        data_element_node = re.sub(r'{.*}', '', element.tag)

        src_cd_description = element.get('displayName')

        src_cd = element.get('code')
        if src_cd is not None:
            src_cd = src_cd.strip()

        codeSystem = element.get('codeSystem')

        resource = element.get('codeSystemName')

        record = {
            "data_source": data_source,
            "resource": resource,
            "data_element_path": data_element_path,
            "data_element_node": data_element_node,
            "codeSystem": codeSystem,
            "src_cd": src_cd,
            "src_cd_description": src_cd_description,
            "src_cd_unit": None, 
            "src_cd_count": None,
            "notes": None,
            "counts": None
        }
        row_list.append(record)

    return row_list


@configure(profile=['DRIVER_MEMORY_LARGE', 'NUM_EXECUTORS_64' ])
@transform(
    discovered_codes = Output("ri.foundry.main.dataset.160c32e8-774e-4e71-b137-766cfc2bdccc"),
    xml_files=Input("ri.foundry.main.dataset.8c8ff8f9-d429-4396-baed-a3de9c945f49")
    #xml_files=Input("ri.foundry.main.dataset.ca873ab5-748b-4f53-9ae4-0c819c7fa3d4")
    #xml_files=Input("ri.foundry.main.dataset.119054ed-4719-4d84-99ba-43625bcafd0f")
)
def compute(ctx, discovered_codes, xml_files):

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

                record_list = snoop_xml_string(file_status.path, xml_content)

                for record_dict in record_list:
                    yield(Row(**record_dict))

    files_df = xml_files.filesystem().files('**/*.xml')
    rdd = files_df.rdd.flatMap(process_file)
    processed_df = rdd.toDF(discovered_codes_schema)
    discovered_codes.write_dataframe(processed_df) 
