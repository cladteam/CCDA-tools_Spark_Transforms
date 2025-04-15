
"""
    vocab_snooper.py
    
    INPUT: 
       - (-d) resources directory of CCDA XML files
       - (-df) dataset of files
       - (-ds) dataset of strings
       - (-f) individual file
    OUTPUT: Foundry datasets:
        "vocab_discovered_codes_with_counts"
        "vocab_discovered_codes_expanded" 
        
            /All of Us-cdb223/HIN - HIE/CCDA/IdentifiedData/eHx_responses/site-smoke-test-responses
"""

import argparse
import logging
import re
import lxml.etree as ET
import os
from xml_ns import ns
#from vocab_map_file import oid_map
import vocab_maps
import pandas as pd
from pathlib import Path
from foundry.transforms import Dataset
from collections import defaultdict 
import tempfile

logger = logging.getLogger(__name__)

# Global DataFrame to hold codes found
# Define the list with column headers for the DataFrame, sourced from 
#  /All of Us-cdb223/Identified: HIN - HIE/CCDA/transform/mapping-reference-files/ccda-value-set-mapping-table
# for comparision, edit as necessary per requirements.
columns = [
    "data_source",        # CCDA XML filename
    "resource",           # @codeSystemName
    "data_element_path",  # XPath to element
    "data_element_node",  # just the last tag
    "codeSystem",         # @codeSystem, vocabulary OID
    "src_cd",             # @code
    "src_cd_description", # @displayName
    "src_cd_unit",        # n/a
    "src_cd_count", 
    "notes", 
    "counts"
]

concepts_introduced_in_mapping = [
    {   # pregnancy
        "data_source": "mapping",
        "resource":    "mapping",
        "data_element_path": None ,
        "data_element_node": None, 
        "codeSystem": "2.16.840.1.113883.6.96",        
        "src_cd": "289908002",           
        "src_cd_description": None ,
        "src_cd_unit": None,
        "src_cd_count": None, 
        "notes": None, 
        "counts": None
    }
]




def snoop_for_code_tag(tree, expr):
    """
    Finds all elements matching the XPath expression (expr) in the 
    XML tree and extracts relevant attributes.
    Appends the extracted information in a dataframe.
    """
    element_list = tree.xpath(expr) 
    ## vocab_codes = pd.DataFrame(columns=columns)
    ele_count=0

    data_element_path_list=[]   
    data_element_node_list=[]  
    src_cd_list=[]   
    src_cd_unit_list=[]   
    codeSystem_list=[]
    resource_list=[] 
    src_cd_description_list=[]
    
    for element in element_list:
        ele_count += 1

        element_path = tree.getelementpath(element)
        # Extract attributes, simplify path, remove namespace and conditionals
        data_element_path = re.sub(r'{.*?}', '', element_path)
        data_element_path = re.sub(r'\[.*?\]', '', data_element_path)
        data_element_path_list.append(data_element_path)
        
        data_element_node = re.sub(r'{.*}', '', element.tag)
        data_element_node_list.append(data_element_node)

        src_cd_description = element.get('displayName')
        src_cd_description_list.append(src_cd_description)

        src_cd = element.get('code')
        if src_cd is not None:
            src_cd = src_cd.strip()
        src_cd_list.append(src_cd)

        codeSystem = element.get('codeSystem')
        codeSystem_list.append(codeSystem)

        resource = element.get('codeSystemName')
        resource_list.append(resource)
 
#        print(f" {element_path}" )

    # don't need units, and this looks suspiciously slowness inducing
#        src_cd_unit=None
#        if  element is not None:
#            for sibling in element.itersiblings():
#                if sibling.tag == '{urn:hl7-org:v3}value':
#                    src_cd_unit = sibling.get('unit')
#        src_cd_unit_list.append(src_cd_unit)

    data_source_list=[None] * ele_count
    src_cd_count_list=[None] * ele_count
    notes_list=[None] * ele_count
    counts_list=[None] * ele_count

    vocab_codes = pd.DataFrame({
        "data_source": data_source_list,
        "resource": resource_list,
        "data_element_path": data_element_path_list,
        "data_element_node": data_element_node_list,
        "codeSystem": codeSystem_list,
        "src_cd": src_cd_list,
        "src_cd_description": src_cd_description_list,
        "src_cd_unit": src_cd_unit_list, 
        "src_cd_count": src_cd_count_list,
        "notes": notes_list,
        "counts": counts_list
        })

#### DEBUG
#                    
#        # Use iterancestors() to find <doseQuantity> in any ancestor
#        # Why not a directpath of sorts to a substanceAdministartion/doseQuantity?
#        # why does the above sibling code not work? FIX TODO
#
#        if element is not None:
#            for ancestor in element.iterancestors():
#                if ancestor.tag in ('{urn:hl7-org:v3}author',
#                                    '{urn:hl7-org:v3}informant',
#                                    '{urn:hl7-org:v3}entryRelationship',
#                                    '{urn:hl7-org:v3}entry',
#                                    '{urn:hl7-org:v3}routeCode'):
#                    break  # why kill the loop?
#                if ancestor.tag == '{urn:hl7-org:v3}substanceAdministration':
#                    dose_quantity_element = ancestor.find('.//{urn:hl7-org:v3}doseQuantity')
#                    if dose_quantity_element is not None:
#                        src_cd_unit = dose_quantity_element.get('unit')
#                        break


    return vocab_codes


def add_concepts_introduced_in_mapping(df):
    new_concepts_df = pd.DataFrame(concepts_introduced_in_mapping)
    df = pd.concat([df, new_concepts_df], ignore_index=True)
    return df
    

def process_xml_file(file_path):
    """
    Process a single XML file, extract elements with codeSystem attributes
    Returns dataframe create in snoop_for_code_tag()
    """
    try:
        logger.info(f"ET parsing {file_path}")
        tree = ET.parse(file_path)
    except FileNotFoundError as e:
        logger.error(f"File {file_path} not found. {e}")
        return pd.DataFrame() 
    except ET.XMLSyntaxError as se:
        logger.error(f"Failed to parse {file_path}. {se}")
        return pd.DataFrame() 
    except Exception as o:
        logger.error(f"Failed to parse {file_path}. {o}")
        return pd.DataFrame() 

    vocab_codes = snoop_for_code_tag(tree, ".//*[@codeSystem]")
    vocab_codes['data_source'] = os.path.basename(file_path)

    vocab_codes  = add_concepts_introduced_in_mapping(vocab_codes)

    return vocab_codes


def process_directory(directory):
    """
    Process all XML files in a directory and return a combined DataFrame.
    """
    all_vocab_codes = pd.DataFrame(columns=columns)

    # Loop through all XML files in the directory
    for file_path in Path(directory).rglob("*.xml"):
        print(f"Processing file: {file_path}")
        file_vocab_codes = process_xml_file(file_path)
        all_vocab_codes = pd.concat([all_vocab_codes, file_vocab_codes], ignore_index=True)

    return all_vocab_codes


def process_dataset_of_files(dataset_name):
    all_vocab_codes = pd.DataFrame(columns=columns)
    
    ccda_documents = Dataset.get(dataset_name)
    ccda_documents_generator = ccda_documents.files()    
    i=0
    for filegen in ccda_documents_generator:
        i+=1
        if i%100 == 0:
            print(f"file number: {i}")

        filepath = filegen.download()
        file_vocab_codes = process_xml_file(filepath)
        all_vocab_codes = pd.concat([all_vocab_codes, file_vocab_codes], ignore_index=True)
        
    return all_vocab_codes


def process_dataset_of_strings(dataset_name):
    print(f"DATA SET NAME: {dataset_name}")
    all_vocab_codes = pd.DataFrame(columns=columns)
    
    ccda_ds = Dataset.get(dataset_name)
    ccda_df = ccda_ds.read_table(format='pandas')
    #'timestamp', 'mspi', 'site', 'status_code', 'response_text',
    # FOR EACH ROW
    if True:
        text=ccda_df.iloc[0,4]
        print("====")
        doc_regex = re.compile(r'(<ClinicalDocument.*?</ClinicalDocument>)', re.DOTALL)
        # (don't close the opening tag because it has attributes)
        # works: doc_regex = re.compile(r'(<section>.*?</section>)', re.DOTALL)
        # FOR EACH "DOC" in this row (hopefully just 1)
        i=0
        for match in doc_regex.finditer(text):
            match_tuple = match.groups(0)
            print(f"LENGTH {i}  {len(match_tuple[0])}")
            with open(f"/home/user/repo/debug_file_{i}.xml", 'w') as f:
                f.write(match_tuple[0])
            i=i+1
                
            with tempfile.NamedTemporaryFile() as temp:
                file_path = temp.name
                with open(file_path, 'w') as f:
                    f.write(match_tuple[0]) # .encode())
                    f.seek(0)
                    
                    file_vocab_codes = process_xml_file(file_path)
                    all_vocab_codes = pd.concat([all_vocab_codes, file_vocab_codes], ignore_index=True)
                    
    return all_vocab_codes

    
def create_derived_datasets(vocab_discovered_codes_expanded):
    """
    Using the "expanded" table, this creates the narrow version and the one with counts,
    and writes CSVs for all three.
    """
    vocab_discovered_codes_expanded.to_csv("vocab_discovered_codes_expanded.csv")
    
    vocab_discovered_codes = vocab_discovered_codes_expanded[['src_cd', 'codeSystem']].drop_duplicates()
    vocab_discovered_codes.to_csv("vocab_discovered_codes.csv")
    
    # count code, codeSystem pairs.
    vocab_discovered_codes_with_counts = \
         vocab_discovered_codes_expanded.groupby(['src_cd', 'codeSystem'])\
                                        .size()\
                                        .reset_index(name='counts')
    vocab_discovered_codes_with_counts.to_csv("vocab_discovered_codes_with_counts.csv")
    
    return(vocab_discovered_codes_with_counts, vocab_discovered_codes)


def export_to_hdfs(codes, codes_with_counts, codes_expanded):
        print("exporting vocab_discovered_codes")
        ds = Dataset.get("vocab_discovered_codes")
        ds.write_table(codes)
        
        print("exporting vocab_discovered_codes_with_counts")
        ds = Dataset.get("vocab_discovered_codes_with_counts")
        ds.write_table(codes_with_counts)
        
        print("exporting vocab_discovered_codes_expanded")
        ds = Dataset.get("vocab_discovered_codes_expanded")
        ds.write_table(codes_expanded)


def code_entry_point_files(dataset_name):
    """ 
    Similar to main() but for calling from code, not command line
    This one is for processing a dataset of files.
    """

    logging.basicConfig(
        format='%(levelname)s: %(message)s',
        filename="log_vocab_snooper.log",
        force=True, level=logging.WARNING)

    vocab_discovered_codes_expanded = process_dataset_of_files(dataset_name)
    vocab_discovered_codes_expanded.drop_duplicates(inplace=True)
    (vocab_discovered_codes_with_counts, vocab_discovered_codes) = \
        create_derived_datasets(vocab_discovered_codes_expanded)
    export_to_hdfs(vocab_discovered_codes,
                   vocab_discovered_codes_with_counts, 
                   vocab_discovered_codes_expanded)
    
def code_entry_point_strings(dataset_name):
    """ 
    Similar to main() but for calling from code, not command line
    This one is for processing a dataset of strings.
    """

    logging.basicConfig(
        format='%(levelname)s: %(message)s',
        filename="log_vocab_snooper.log",
        force=True, level=logging.WARNING)

    vocab_discovered_codes_expanded = process_dataset_of_strings(dataset_name)
    vocab_discovered_codes_expanded.drop_duplicates(inplace=True)
    (vocab_discovered_codes_with_counts, vocab_discovered_codes) = \
        create_derived_datasets(vocab_discovered_codes_expanded)
#    export_to_hdfs(vocab_discovered_codes,
#                   vocab_discovered_codes_with_counts, 
#                   vocab_discovered_codes_expanded)

def main():
    """
    Main function that parses arguments, processes the XML file(s), extracts code elements, and cleans up the DataFrame.
    """
    parser = argparse.ArgumentParser(
        prog='CCDA - OMOP Code Snooper',
        description="Finds all code elements and shows what concepts they represent",
        epilog='epilog?')
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument('-f', '--filename', help="Filename of the XML file to parse")
    group.add_argument('-d', '--directory', help="Directory containing XML files to parse")
    parser.add_argument('-x', '--export', action=argparse.BooleanOptionalAction, help="export to foundry and write CSV")
    group.add_argument('-ds', '--dataset_strings', help="dataset of strings to parse")
    group.add_argument('-df', '--dataset_files', help="dataset of files to parse")
    args = parser.parse_args()

    logging.basicConfig(
        format='%(levelname)s: %(message)s',
        filename="log_vocab_snooper.log",
        force=True, level=logging.WARNING)

    all_vocab_codes = pd.DataFrame()

    if args.filename:
        print(f"Processing single file: {args.filename}")
        vocab_discoverd_codes_expanded = process_xml_file(args.filename)
    elif args.directory:
        print(f"Processing all files in directory: {args.directory}")
        vocab_discovered_codes_expanded = process_directory(args.directory)
    elif args.dataset_files:
        vocab_discovered_codes_expanded = process_dataset_of_files(args.dataset_files)
    elif args.dataset_strings:
        vocab_discovered_codes_expanded = process_dataset_of_strings(args.dataset_strings)
    

    # Output Datasets to Foundry HDFS
    vocab_discovered_codes_expanded.drop_duplicates(inplace=True)
    if args.export:
        (vocab_discovered_codes_with_counts, vocab_discovered_codes) = \
        create_derived_datasets(vocab_discovered_codes_expanded)
        export_to_hdfs(vocab_discovered_codes, 
                       vocab_discovered_codes_with_counts, 
                       vocab_discovered_codes_expanded)

    

if __name__ == '__main__':
    main()

#      process_dataset_of_files('ccda_documents')
#     code_entry_point_strings('ehx_ccda_response_example_copy')
#    code_entry_point_files('ccda_response_files')
