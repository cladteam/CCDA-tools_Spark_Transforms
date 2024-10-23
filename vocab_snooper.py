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

def get_code_name(codeSystem, codeSystemName, code, concept_df):
    """
    Retrieves the concept name from a concept DataFrame based on the given code system, code system name, and code.
    Returns 'n/a' if no match is found.
    """
    if codeSystemName:
        vocabulary_id = re.sub(r' CT', '', codeSystemName)  # Remove unwanted text from codeSystemName
        concept_query = f"concept_code == '{code}' and vocabulary_id == '{vocabulary_id}'"
        concept_row = concept_df.query(concept_query)

        if concept_row.size > 1:
            concept_name = concept_row['concept_name'].values[0]
            vocabulary_id = concept_row['vocabulary_id'].values[0]
            return concept_name, vocabulary_id
        return 'n/a', 'n/a'
    return 'n/a', 'n/a'


def snoop_for_code_tag(tree, expr, concept_df):
    """
    Finds all elements matching the XPath expression (expr) in the XML tree and extracts relevant attributes.
    Appends the extracted information to the given vocab_codes DataFrame.
    """
    section_elements = tree.findall(expr, ns)
    vocab_codes = pd.DataFrame(columns=columns)
    for section_element in section_elements:
        # Extract attributes
        data_element_node = re.sub(r'{.*}', '', section_element.tag)
        src_cd = section_element.attrib.get('code')
        codeSystem = section_element.attrib.get('codeSystem')
        resource = section_element.attrib.get('codeSystemName')
        src_cd_description = section_element.attrib.get('displayName')
        #templateId = section_element.attrib.get('templateId')

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


def process_xml_file(file_path, concept_df):
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


    vocab_codes = snoop_for_code_tag(tree, ".//*[@codeSystem]", concept_df)
    vocab_codes['data_source'] = os.path.basename(file_path)

    return vocab_codes


def process_directory(directory, concept_df):
    """
    Process all XML files in a directory and return a combined DataFrame.
    """
    all_vocab_codes = pd.DataFrame(columns=columns)

    # Loop through all XML files in the directory
    for file_path in Path(directory).rglob("*.xml"):
        print(f"Processing file: {file_path}")
        file_vocab_codes = process_xml_file(file_path, concept_df)
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

    print("Reading Vocabulary, this may take a minute...")
    # Load concept DataFrame (assuming you have this functionality available)
    concept_df = [] #vocab_maps.read_concept()

    all_vocab_codes = pd.DataFrame()

    if args.filename:
        print(f"Processing single file: {args.filename}")
        all_vocab_codes = process_xml_file(args.filename, concept_df, ns)
    elif args.directory:
        print(f"Processing all files in directory: {args.directory}")
        all_vocab_codes = process_directory(args.directory, concept_df)

    # count code, codeSystem pairs.
    counts_df = all_vocab_codes.groupby(['src_cd', 'codeSystem']).size().reset_index(name='counts')

    # Output Datasets to Foundry HDFS
    vocab_discovered_codes_expanded = Dataset.get("vocab_discovered_codes_with_counts")
    vocab_discovered_codes_expanded.write_table(counts_df)

    vocab_discovered_codes_expanded = Dataset.get("vocab_discovered_codes_expanded")
    vocab_discovered_codes_expanded.write_table(all_vocab_codes)
    

if __name__ == '__main__':
    main()
