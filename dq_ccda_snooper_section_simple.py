"""
    dq_ccda_snooper_section.py
    
    Snoops a dataset of CCDA XML files for section info used in DQ.
    
    Output: dq_ccda_snooper_section dataset
"""   

import os
import pandas as pd
import argparse
import logging
import re
import lxml.etree as ET
from xml_ns import ns
from collections import defaultdict
from foundry.transforms import Dataset

logger = logging.getLogger(__name__)


# Output dataset columns
df_columns = [  
    'source',         # Name of the file being processed
    'section',          # OID of the section within the document (e.g. 2.16.840.1.113883.10.20.22.2.5.1)
    'section_code',     # Code representing the section (e.g. 11450-4)
    'section_name',     # Human-readable name of the section (e.g. PROBLEM LIST)
    'codeSystem',       # OID system identifier (e.g., 2.16.840.1.113883.6.96 = SNOMED CT)
    'code',             # Specific code within the code system
    'value_type',       # Type of value (e.g., numeric, text, coded entry)
    'value_unit',       # Unit of measurement (if applicable)
    'value_value',      # Actual value extracted
    'value_code',       # Code associated with the value (if applicable)
    'value_codeSystem', # Code system for the value
    'value_text',       # Human-readable text representation of the value
    'path'              # XML path to the extracted data point
]

#todo, convert to excluse list from dataset
code_exclusion_list = [
'33999-4'
]


def snoop_for_tag(starting_ele, tag, tree):
    """
        finds all tagged elements below the passed-in starting_ele

        parameter: tag is just the simple name of the tag like code or value
        returns:  Dict of (attribute-dict, text-value) pairs, keyed by path
        returns:  Dict  path -> (attribute-dict, text-value) 
        returns:  {path:( {attr: attr-value}, text-value)  }
    """

    
    
### PROBLEM: I coded without this list!!!    
####    value_dict = defaultdict(list)
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

        value_dict[value_path].append((value_attribs_dict, value_ele.text))

    return value_dict

#returned
#defaultdict(<class 'list'>, 
#            {'component/structuredBody/component[1]/section/entry[1]/act': 
#n             [(defaultdict(<class 'str'>, {
#                 'code': '48768-6',
#                 'displayName': 'Payment Sources', 
#                 'codeSystem': '2.16.840.1.113883.6.1', 
#                 'codeSystemName': 'LN'}
#                          ), None)], 
#             'component/structuredBody/component[1]/section/entry[1]/act/entryRelationship/act': [(defaultdict(<class 'str'>, {'nullFlavor': 'OTH'}), #None)], 
#             'component/structuredBody/component[1]/section/entry[1]/act/entryRelationship/act/performer/assignedEntity': [(defaultdict(<class 'str'>, ##{'code': 'PAYOR', 'displayName': 'Payor', 'codeSystem': '2.16.840.1.113883.5.110', 'codeSystemName': 'RoleClass'}), None)], 
#            'component/structuredBody/component[1]/section/entry[1]/act/entryRelationship/act/participant[1]/participantRole': [(defaultdict(<class #'str'>, {'code': 'SEL', 'codeSystem': '2.16.840.1.113883.5.111', 'codeSystemName': 'RoleCode', 'displayName': 'Self'}), None)], 
#             'component/structuredBody/component[1]/section/entry[1]/act/entryRelationship/act/entryRelationship/act': [(defaultdict(<class 'str'>, ##{'nullFlavor': 'UNK'}), None)]})


def snoop_sections(tree, file_path):
    # Initialize an empty list
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
            code_dict = snoop_for_tag(entry_ele, "code", tree)
            value_dict = snoop_for_tag(entry_ele, "value", tree)
            print(code_dict)
            #   {path:( {attr: attr-value}, text-value)  }
            for code_path in code_dict:
                record = {
                    'source'      : os.path.basename(file_path), 
                    'section'     : section_template_id,
                    'section_code': section_code,
                    'section_name': section_name,
                    'path'        : code_path,

                    'code'        : code_dict[code_path][0]['code'],
                    'codeSystem'  : code_dict[code_path][0]['codeSystem'],

                    'value_type'  : value_dict[code_path][0]['type'],
                    'value_unit'  : value_dict[code_path][0]['unit'],
                    'value_value' : value_dict[code_path][0]['value'],
                    'value_code'  : value_dict[code_path][0]['code'], 
                    'value_codeSystem': value_dict[code_path][0]['codeSystem'],

                    #'value_text'  : code_dict[path][1].strip() 
                    'value_text'  : code_dict[code_path][1].strip() 
                }
                records.append(record)

            # Do the paths in value_dict not in code_dict
            for code_path in (value_dict.keys() - code_path.keys()):       
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

    return(records)



##            ##code_value_dict = defaultdict(list)
##            ### MERGE?? why? you just pull them apart laer
##            #https://stackoverflow.com/questions/38987/how-do-i-merge-two-dictionaries-in-a-single-expression-in-python
##            #for d in (code_dict, value_dict):
##            #    for key, value in d.items():
##            #        code_value_dict[key].extend(value)  # Preserve list values
##            # code_dict and value_dict are:  {path:( {attr: attr-value}, text-value)  }
##            # result here is {path: [ {attr: attr-value}, text-value) ] }
##
##            for code_path in code_dict:
##                # Retrieve corresponding value(s) for the code (if any)
##                code_value_tuple_list = code_value_dict[code_path]  # Tuple contains (attributes dictionary, text content)
##                code_value_tuple_list = [t for t in code_value_tuple_list if 'nullFlavor' not in t[0]]
##
##                for code_value_tuple in code_value_tuple_list:
##                    # Construct a new row for the DataFrame
##                    record = {
##                        'source': os.path.basename(file_path), # [12:29],
##                        'section': section_template_id,
##                        'section_code': section_code,
##                        'section_name': section_name,
##                        'path': code_path,
#          ....and here, we have these elaborate data structures, but now we're digging back into code_ele?
##                        'code': code_ele.get('code',''),
##                        'codeSystem': code_ele.get('codeSystem',''),

##                        'value_type': code_value_tuple[0].get('type', ''),  # Extract 'type' if available
##                        'value_unit': code_value_tuple[0].get('unit', ''),  # Extract 'unit' if available
##                        'value_value': code_value_tuple[0].get('value', ''),  # Extract 'value' if available
##                        'value_code': code_value_tuple[0].get('code', ''),  # Extract 'code' if available
##                        'value_codeSystem': code_value_tuple[0].get('codeSystem', ''),  # Extract 'codeSystem' if available
##                        'value_text': code_value_tuple[1].strip() if code_value_tuple[1] else ''  # Extract and clean text content
##                    }
##                   
##                        records.append(record)
            

#### TODO ? def snoop_sections_without_entries():


#def snoop_section(tree, filename:str):
def process_xml_file(file_path):
    """ returns a list of dictionaries/records of attributes from 
        persons in the file
    """
    data_records = []
    with open(file_path, 'rb') as file:
        tree = ET.parse(file_path)
        try:
            logger.info(f"ET parsing {file_path}")
            tree = ET.parse(file_path)
        except FileNotFoundError:
            logger.error(f"File {file_path} not found.")
            return data_records
        except ET.XMLSyntaxError as e:
            logger.error(f"SyntaxError: Failed to parse (syntax error) {file_path}. {e}")
            return data_records
        except Exception as e:
            logger.error(f"Exception: Failed to parse (other) {file_path} {e}")
            return data_records

    
        """
        Extracts structured data from CCDA XML sections and returns a DataFrame.
        Returns: pd.DataFrame
        """
    
        return snoop_sections(tree, file_path)
    
    
     

def process_dataset_of_files_by_name(dataset_name):
    """
        snoops each file for records
        returns a dataframe of records
    """
    ccda_documents = Dataset.get(dataset_name)
    ccda_documents_generator = ccda_documents.files()    
    all_records=[]
    for filegen in ccda_documents_generator:
        filepath = filegen.download()
        
        record_list = process_xml_file(filepath)
        
        all_records += record_list
    df = pd.DataFrame(all_records)
    return df


def entry_point(dataset_read, dataset_write, export_flag, write_flag):
    """
    A function to call into this functionality, useful for
    calling from a notebook. 
    Also used below for the script interface.
    
    dataset_name is the name of a dataset as seen in the Data tab in Foundry.
    """

    logging.basicConfig(
        format='%(levelname)s: %(message)s',
        filename="log_dq_snooper_section.log",
        force=True, level=logging.WARNING)

    df = process_dataset_of_files_by_name(dataset_read)

    if export_flag:
        # Save dataset to HDFS/Spark in Foundry
        dq_ccda_snooper_section = Dataset.get(dataset_write)
        dq_ccda_snooper_section.write_table(df)
        print(f"wrote {dataset_write} dataset")
        
    if write_flag:
        df.to_csv(f"{dataset_write}.csv")
        
