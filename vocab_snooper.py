
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
from vocab_map_file import oid_map
import vocab_maps
import pandas as pd
from pathlib import Path
from foundry.transforms import Dataset
from collections import defaultdict

# mamba install -y -q lxml


# Global DataFrame to hold codes found
# Define the list with column headers for the DataFrame, sourced from /All of Us-cdb223/Identified: HIN - HIE/CCDA/transform/mapping-reference-files/ccda-value-set-mapping-table
# for comparision, edit as necessary per requirements.
columns = [
    "data_source", "resource", "data_element_path", "data_element_node", 
    "codeSystem", "src_cd", "src_cd_description","src_cd_count", "target_concept_id", 
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
        # Extract attributes
        data_element_node = re.sub(r'{.*}', '', element.tag)
        src_cd_description = element.attrib.get('displayName')
        src_cd = element.get('code')
        codeSystem = element.get('codeSystem')
        resource = element.get('codeSystemName')
        src_cd_description = element.get('displayName')

        element_path = tree.getelementpath(element)


        # Append to vocab_codes DataFrame
        new_row = pd.DataFrame([{
            'data_element_node': data_element_node,
            'src_cd': src_cd,
            'codeSystem': codeSystem,
            'resource': resource,
            'src_cd_description': src_cd_description,

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



def main():
    """
    Main function that parses arguments, processes the XML file(s), extracts code elements, and cleans up the DataFrame.
    """
    parser = argparse.ArgumentParser(
        prog='CCDA - OMOP Code Snooper',
        description="Finds all code elements and shows what concepts they represent",
        epilog='epilog?')
    parser.add_argument('-f', '--filename', help="Filename of the XML file to parse")
    parser.add_argument('-d', '--directory', help="Directory containing XML files to parse")
    args = parser.parse_args()

    if not args.filename and not args.directory:
        print("Error: You must provide either a filename or a directory.")
        return

    all_vocab_codes = pd.DataFrame()

    if args.filename:
        print(f"Processing single file: {args.filename}")
        all_vocab_codes = process_xml_file(args.filename)
    elif args.directory:
        print(f"Processing all files in directory: {args.directory}")
        all_vocab_codes = process_directory(args.directory)
        

    all_vocab_codes.to_csv("vocab_codes.csv")
    # count code, codeSystem pairs.
    counts_df = all_vocab_codes.groupby(['src_cd', 'codeSystem']).size().reset_index(name='counts')
    counts_df.drop_duplicates().to_csv("counts_deduped.csv")
    counts_df.to_csv("counts.csv")
    

    # Output Datasets to Foundry HDFS
    if False:
        vocab_discovered_codes_expanded = Dataset.get("vocab_discovered_codes_with_counts")
        vocab_discovered_codes_expanded.write_table(counts_df)

        vocab_discovered_codes_expanded = Dataset.get("vocab_discovered_codes_expanded")
        vocab_discovered_codes_expanded.write_table(all_vocab_codes)
    

if __name__ == '__main__':
    main()
