from transforms.api import transform, Input, Output, configure
from pyspark.sql import types as  T
from pyspark.sql import Row
import io
import re
from lxml import etree as ET
from ..xml_ns import ns
import os

people_snooper_schema =  T.StructType([
    T.StructField("Filename", T.StringType(), True),
    T.StructField("Gender", T.StringType(), True),
    T.StructField("Race", T.StringType(), True),
    T.StructField("Ethnicity", T.StringType(), True),
    T.StructField("Birthtime", T.StringType(), True),
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

    data_records = []

    record = {'Filename': os.path.basename(file_path)}
    for tag, label in elements:
        element = tree.find(f'.//hl7:{tag}', namespaces=ns)
        
        if tag == 'birthTime' and element is not None:
            record[label] = element.get('value')
        elif element is not None:
            record[label] = element.get('displayName')
        else:
            record[label] = 'Unknown'

        data_records.append(record)

    return data_records

@configure(profile=['DRIVER_MEMORY_LARGE', 'NUM_EXECUTORS_64' ])
@transform(
    snooper_people = Output("ri.foundry.main.dataset.7062bd72-cbff-425f-ba8f-c03d892f58d9"),
    # xml_files=Input("ri.foundry.main.dataset.ca873ab5-748b-4f53-9ae4-0c819c7fa3d4")
    xml_files=Input("ri.foundry.main.dataset.119054ed-4719-4d84-99ba-43625bcafd0f")
)
def compute(snooper_people, xml_files):

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

                record_list = parse_string(file_status.path, xml_content)

                for record_dict in record_list:
                    yield(Row(**record_dict))

    files_df = xml_files.filesystem().files('**/*.xml')
    rdd = files_df.rdd.flatMap(process_file)
    processed_df = rdd.toDF(people_snooper_schema)
    snooper_people.write_dataframe(processed_df)
