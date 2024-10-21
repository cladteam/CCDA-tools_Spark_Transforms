#!/usr/bin/env python3
"""
    raw_section_snooper - looks for ANY sections (XML elemnent tagged "section"?),
        within each type:
        - entry: ;shows template Id, concepts and names if present,
        - entry/organizer: ;shows template Id, concepts and names if present,
        - title
        - code
        - value TBD

"""

"""

"""
import argparse
import re
import xml.etree.ElementTree as ET  # https://docs.python.org/3/library/xml.etree.elementtree.html
from xml_ns import ns
from vocab_map_file import oid_map
import vocab_maps
import pandas as pd
#experimental codeSystem lookup, likely cannot use it in Foundry
from oid_lookup import get_oid_title


# Global DataFrame to hold codes found
vocab_codes = pd.DataFrame(columns=['tag', 'code', 'codeSystem', 'codeSystemName', 'displayName', 'templateId'])

def get_code_name(codeSystem, codeSystemName, code, concept_df):
    """
    Retrieves the concept name from a concept DataFrame based on the given code system, code system name, and code.
    Returns 'n/a' if no match is found.
    """
    if codeSystemName:
        vocabulary_id = re.sub(r' CT', '', codeSystemName)  # Remove unwanted text from codeSystemName
        concept_query = f"concept_code == '{code}' and vocabulary_id == '{vocabulary_id}'"
        concept_row = concept_df.query(concept_query)

        # If a match is found, return the concept name; otherwise, return 'n/a'
        if concept_row.size > 1:
            return concept_row['concept_name'].values[0]
        return 'n/a'
    return 'n/a'

def snoop_for_code_tag(tree, expr, ns,concept_df):
    """
    Finds all elements matching the XPath expression (expr) in the XML tree and extracts relevant attributes.
    Appends the extracted information to the global vocab_codes DataFrame.
    """
    section_elements = tree.findall(expr, ns)

    # Use the global vocab_codes DataFrame
    global vocab_codes

    # Loop through each element matching the expression
    for section_element in section_elements:
        
        # Extract relevant attributes from the document
        tag = re.sub(r'{.*}', '', section_element.tag)
        code = section_element.get('code')
        codeSystem = section_element.get('codeSystem')
        codeSystemName = section_element.get('codeSystemName')
        displayName = section_element.get('displayName')
        templateId = section_element.get('templateId')

        # Print extracted details for debugging or logging purposes
        print((f"\nSECTION  {type(section_element)} "
               f" tag:{tag} "
               f" code: {code} "
               f" codeSystem: {codeSystem} "
               f" codeSystemName: {codeSystemName} "
               f" displayName: {displayName} "
               f" templateId: {templateId}"))

        # Create a new DataFrame row
        new_row = pd.DataFrame([{
            'tag': tag,
            'code': code,
            'codeSystem': codeSystem,
            'codeSystemName': codeSystemName,
            'displayName': displayName,
            'templateId': templateId
        }])

        # Append the new row to vocab_codes using pd.concat for better performance
        vocab_codes = pd.concat([vocab_codes, new_row], ignore_index=True)

def main():
    """
    Main function that parses arguments, processes the XML file, extracts code elements, and cleans up the DataFrame.
    """
    parser = argparse.ArgumentParser(
        prog='CCDA - OMOP Code Snooper',
        description="Finds all code elements and shows what concepts they represent",
        epilog='epilog?')
    parser.add_argument('-f', '--filename', required=True, help="Filename of the XML file to parse")
    args = parser.parse_args()

    print("Reading Vocabulary, this may take a minute...")
    concept_df = vocab_maps.read_concept()  # Uncomment if you need to load a concept DataFrame

    # Parse the XML file
    tree = ET.parse(args.filename)

    # Extract code elements (tag-based and attribute-based)
    snoop_for_code_tag(tree, ".//code", ns,concept_df)
    snoop_for_code_tag(tree, ".//*[@codeSystem]", ns,concept_df)

    # Clean up the DataFrame
    # Step 1: Remove duplicates
    vocab_codes_cleaned = vocab_codes.drop_duplicates()

    # Step 2: Sort the DataFrame by 'codeSystem'
    vocab_codes_cleaned = vocab_codes_cleaned.sort_values(by='codeSystem')

    # Pass the cleaned DataFrame into the get_code_name function to fetch concept names
    for index, row in vocab_codes_cleaned.iterrows():
        # If codeSystemName is missing, look it up using the codeSystem (OID)
        if pd.isna(row['codeSystemName']) and row['codeSystem']:
            oid = row['codeSystem']
            # Fetch the title for the OID (codeSystem) using get_oid_title
            title = get_oid_title(oid)
            vocab_codes_cleaned.at[index, 'codeSystemName'] = title  # Update codeSystemName with the title
        # Fetch concept_name using get_code_name
        concept_name = get_code_name(row['codeSystem'], row['codeSystemName'], row['code'], concept_df)
        vocab_codes_cleaned.at[index, 'concept_name'] = concept_name

    # Pass the cleaned DataFrame into the get_code_name function to fetch concept names

        
    # Output the cleaned DataFrame
    # Optionally, you can save the DataFrame to a CSV file
    vocab_codes_cleaned.to_csv('raw_vocab_codes.csv', index=False)
    print(vocab_codes_cleaned)

if __name__ == '__main__':
    main()
