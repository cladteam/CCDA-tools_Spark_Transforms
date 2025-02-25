
"""
    vocab_snooper.py
    
    INPUT: resources directory of CCDA XML files
    OUTPUT: Foundry datasets:
        "vocab_discovered_codes_with_counts"
        "vocab_discovered_codes_expanded" 
"""

import argparse
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

# mamba install -y -q lxml


# Global DataFrame to hold codes found
# Define the list with column headers for the DataFrame, sourced from 
#  /All of Us-cdb223/Identified: HIN - HIE/CCDA/transform/mapping-reference-files/ccda-value-set-mapping-table
# for comparision, edit as necessary per requirements.
columns = [
    "data_source", "resource", "data_element_path", "data_element_node", 
    "codeSystem", "src_cd", "src_cd_description","src_cd_unit","src_cd_count", "target_concept_id", 
    "target_concept_name", "target_domain_id", "target_vocabulary_id", 
    "target_concept_class_id", "target_standard_concept", "target_concept_code", 
   "target_tbl_column_name", "notes", "counts"
]

def snoop_for_code_tag(tree, expr):
    """
    Finds all elements matching the XPath expression (expr) in the XML tree and extracts relevant attributes.
    Appends the extracted information to the given vocab_codes DataFrame.
    """
    #section_elements = tree.findall(expr, ns)
    element_list = tree.xpath(expr)
    vocab_codes = pd.DataFrame(columns=columns)
    for element in element_list:

        element_path = tree.getelementpath(element)
        # Extract attributes, simplify path, remove namespace and conditionals
        data_element_path = re.sub(r'{.*?}', '', element_path)
        data_element_path = re.sub(r'\[.*?\]', '', data_element_path)
        
        data_element_node = re.sub(r'{.*}', '', element.tag)
        src_cd_description = element.attrib.get('displayName')
        src_cd = element.get('code')
        codeSystem = element.get('codeSystem')
        resource = element.get('codeSystemName')
        src_cd_description = element.get('displayName')
        src_cd_unit = ''

        if element is not None:
            for sibling in element.itersiblings():
                if sibling.tag == '{urn:hl7-org:v3}value':
                    src_cd_unit = sibling.get('unit')               
        
        if element is not None:
            # Use iterancestors() to find <doseQuantity> in any ancestor
            for ancestor in element.iterancestors():
                # Check if the ancestor contains a <doseQuantity> element
                if ancestor.tag in ('{urn:hl7-org:v3}author', '{urn:hl7-org:v3}informant', 
                                    '{urn:hl7-org:v3}entryRelationship',
                                    '{urn:hl7-org:v3}entry','{urn:hl7-org:v3}routeCode'):
                    break
                if ancestor.tag == '{urn:hl7-org:v3}substanceAdministration':
                    dose_quantity_element = ancestor.find('.//{urn:hl7-org:v3}doseQuantity')
                    if dose_quantity_element is not None:
                        src_cd_unit = dose_quantity_element.get('unit')
                        break

# Append to vocab_codes DataFrame
        new_row = pd.DataFrame([{
            'data_element_path': data_element_path,
            'data_element_node': data_element_node,
            'src_cd': src_cd,
            'codeSystem': codeSystem,
            'resource': resource,
            'src_cd_description': src_cd_description,
            'src_cd_unit': src_cd_unit,

        }])
        # Concatenate the new row with the DataFrame
        vocab_codes = pd.concat([vocab_codes, new_row], ignore_index=True)

    return vocab_codes


def process_xml_file(file_path):
    """
    Process a single XML file, extract code elements, and return the resulting DataFrame.
    """
    try:
        tree = ET.parse(file_path)
    except FileNotFoundError:
        print(f"Error: File {file_path} not found.")
        return pd.DataFrame()  # Return an empty DataFrame
    except ET.XMLSyntaxError:
        print(f"Error: Failed to parse {file_path}.")
        return pd.DataFrame()  # Return an empty DataFrame

    #vocab_codes = snoop_for_code_tag(tree, ".//code", concept_df, vocab_codes)
    # would need to concatenate and consider duplicates between the two, while leaving duplicates
    # within on... Do we need both?


    vocab_codes = snoop_for_code_tag(tree, ".//*[@codeSystem]")
    vocab_codes['data_source'] = os.path.basename(file_path)

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
    for filegen in ccda_documents_generator:
        filepath = filegen.download()
        print(f"\n\nPROCESSING {os.path.basename(filepath)}\n")
        file_vocab_codes = process_xml_file(filepath)
        all_vocab_codes = pd.concat([all_vocab_codes, file_vocab_codes], ignore_index=True)

    return all_vocab_codes
            
        
def create_derived_datasets(vocab_discovered_codes_expanded):
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
        ds = Dataset.get("vocab_discovered_codes")
        ds.write_table(codes)
        
        ds = Dataset.get("vocab_discovered_codes_with_counts")
        ds.write_table(codes_with_counts)

        ds = Dataset.get("vocab_discovered_codes_expanded")
        ds.write_table(codes_expanded)

def code_entry_point(dataset_name):
    """ similar to main() but for calling from code, not command line
    """
    vocab_discovered_codes_expanded = process_dataset_of_files(dataset_name)
    (vocab_discovered_codes_with_counts, vocab_discovered_codes) = \
        create_derived_datasets(vocab_discovered_codes_expanded)
    export_to_hdfs(vocab_discovered_codes,
                   vocab_discovered_codes_with_counts, 
                   vocab_discovered_codes_expanded)

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
    parser.add_argument('-x', '--export', action=argparse.BooleanOptionalAction, help="export to foundry")
    group.add_argument('-ds', '--dataset', help="dataset to parse")
    args = parser.parse_args()

    if not args.filename and not args.directory:
        print("Error: You must provide either a filename or a directory.")
        return

    all_vocab_codes = pd.DataFrame()

    if args.filename:
        print(f"Processing single file: {args.filename}")
        vocab_discoverd_codes_expanded = process_xml_file(args.filename)
    elif args.directory:
        print(f"Processing all files in directory: {args.directory}")
        vocab_discovered_codes_expanded = process_directory(args.directory)
    elif args.dataset:
        vocab_discovered_codes_expanded = process_dataset_of_files(args.dataset)
    

    # Output Datasets to Foundry HDFS
    if args.export:
        export_to_hdfs(vocab_discovered_codes, 
                       vocab_discovered_codes_with_counts, 
                       vocab_discovered_codes_expanded)
    

if __name__ == '__main__':
    main()
