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

#def snoop_section(tree, filename:str):
def process_xml_file(file_path):
    """ returns a list of dictionaries/records of attributes from 
        persons in the file
    """
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
        except Error as e:
            logger.error(f"Error: Failed to parse (other) {file_path} {e}")
            return data_records

    
        """
        Extracts structured data from CCDA XML sections and returns a DataFrame.
        Returns: pd.DataFrame
        """
    
        # Initialize an empty list
        records = []
        
        # Find all 'section' elements in the XML tree using the specified namespace
        section_elements = tree.findall(".//section", ns)

        section_i=0
        for section_element in section_elements:
            section_i += 1
            if section_i % 100 == 0:
                print(f"{os.path.basename(file_path)} section: {section_i} \n")

            # Default section template ID if not found
            section_template_id = "n/a"
    
            # Extract templateId from the section (if available)
            section_template_ele = section_element.findall("templateId", ns)
            if len(section_template_ele) > 0:
                section_template_id = section_template_ele[0].get('root')
    
            # Extract section code and display name
            section_code = section_element.findall("code", ns)[0].get('code')
            section_name = section_element.findall("code", ns)[0].get('displayName')
    
            # Find all 'entry' elements within the section
            # FINDALL entry
            entry_elements = section_element.findall("entry", ns)
    
            entry_i=0
            for entry_ele in entry_elements:
                entry_i += 1
                if entry_i % 100 == 0:
                    print(f"{os.path.basename(file_path)} section: {section_i}  entry: {entry_i}  \n")
    
                # Dictionary to store extracted values, grouped by XML path
                value_dict = defaultdict(list)
                code_dict = defaultdict(list)
    
                # Extract all 'value' elements within the entry
                ## FINALL value
                value_elements = entry_ele.findall(".//value", ns)
                value_i=0
                for value_ele in value_elements:
                    value_i += 1
                    if value_i % 100 == 0:
                        print(f"{os.path.basename(file_path)} section: {section_i}  value: {value_i}  \n")
    
                    # Clean the XML path (remove namespace references)
                    value_path = re.sub(r'{.*?}', '', tree.getelementpath(value_ele))
                    value_path = "/".join(value_path.split("/")[:-1])  # Get parent path
    
                    value_attribs_dict = {}
                    for (attr, value) in value_ele.attrib.items():
                        clean_attr = re.sub(r'{.*}', '', attr)
                        clean_value = re.sub(r'{.*}', '', value)
                        value_attribs_dict[clean_attr] = clean_value
    
                    # Dict of (attribute-dict, text-value) pairs, keyed by path
                    value_dict[value_path].append((value_attribs_dict, value_ele.text))

                # Extract all 'code' elements within the entry
                # FINDALL
                code_elements = entry_ele.findall(".//code", ns)
                code_i=0
                for code_ele in code_elements:
                    code_i += 1
                    if code_i % 100 == 0:
                        print(f"{os.path.basename(file_path)} section: {section_i} entry: {entry_i} code:{code_i}  \n")

                    # Clean the XML path (remove namespace references)
                    code_path = re.sub(r'{.*?}', '', tree.getelementpath(code_ele))
                    code_path = "/".join(code_path.split("/")[:-1])  # Get parent path
                    from IPython.display import display
                    code_attribs_dict = {}
                    for (attr, code) in code_ele.attrib.items():
                        clean_attr = re.sub(r'{.*}', '', attr)
                        clean_code = re.sub(r'{.*}', '', code)
                        code_attribs_dict[clean_attr] = clean_code
    
                    # Dict of (attribute-dict, text-value) pairs, keyed by path
                    code_dict[code_path].append((code_attribs_dict, code_ele.text))               
                    
                    code_value_dict = defaultdict(list)
    
                    # Merge code_dict and value_dict
                    # MERGE??
                    for d in (code_dict, value_dict):
                        for key, value in d.items():
                            code_value_dict[key].extend(value)  # Preserve list values
                    
                    # Retrieve corresponding value(s) for the code (if any)
                    code_value_tuple_list = code_value_dict[code_path]  # Tuple contains (attributes dictionary, text content)
                    code_value_tuple_list = [t for t in code_value_tuple_list if 'nullFlavor' not in t[0]]
    
                    for code_value_tuple in code_value_tuple_list:
                        # Construct a new row for the DataFrame
                        record = {
                            'source': os.path.basename(file_path), # [12:29],
                            'section': section_template_id,
                            'section_code': section_code,
                            'section_name': section_name,
                            'path': code_path,
                            'code': code_ele.get('code',''),
                            'codeSystem': code_ele.get('codeSystem',''),
                            'value_type': code_value_tuple[0].get('type', ''),  # Extract 'type' if available
                            'value_unit': code_value_tuple[0].get('unit', ''),  # Extract 'unit' if available
                            'value_value': code_value_tuple[0].get('value', ''),  # Extract 'value' if available
                            'value_code': code_value_tuple[0].get('code', ''),  # Extract 'code' if available
                            'value_codeSystem': code_value_tuple[0].get('codeSystem', ''),  # Extract 'codeSystem' if available
                            'value_text': code_value_tuple[1].strip() if code_value_tuple[1] else ''  # Extract and clean text content
                        }
                       
                        records.append(record)
            
    return records
"""
def main():
  
    #Source documents in a dataset
    ccda_documents = Dataset.get("ccda_documents")
    ccda_documents_files = ccda_documents.files().download()
    
    #Process multiple files in a directory
    if ccda_documents_files is not None:
        # Initialize an empty DataFrame to store results from all files
        all_files_df = pd.DataFrame(columns=df_columns)

        for filename in ccda_documents_files:
            tree = ET.parse(ccda_documents_files[filename])  # Parse the XML file
            file_df = snoop_section(tree, filename)  # Extract structured data

            # Append the new file's data to the cumulative DataFrame
            all_files_df = pd.concat([all_files_df, file_df], ignore_index=True)

        # Save the results to a CSV file
        #all_files_df.to_csv("raw_section_snooper.csv", sep=",", header=True, index=False)
        all_files_df = all_files_df[~all_files_df['code'].isin(code_exclusion_list)]
        df_dq_ccda_snooper_section = all_files_df

        #Save the dataset to Foundry
        dq_ccda_snooper_section = Dataset.get("dq_ccda_snooper_section")
        dq_ccda_snooper_section.write_table(df_dq_ccda_snooper_section)
"""
def process_dataset_of_files(dataset):
    """
        snoops each file for records
        returns a dataframe of records
    """
    ccda_documents_generator = dataset.files()    
    all_records=[]
    i=0
    for filegen in ccda_documents_generator:
        i+=1
        if i%100 == 0:
            print(f"file number: {i}")
        filepath = filegen.download()
        record_list = process_xml_file(filepath)
        all_records += record_list
    df = pd.DataFrame(all_records)
    return df

def process_dataset_of_files_by_name(dataset_name, limit):
    """
        snoops each file for records
        returns a dataframe of records
    """
    ccda_documents = Dataset.get(dataset_name)
    ccda_documents_generator = ccda_documents.files()    
    all_records=[]
    file_count=0
    for filegen in ccda_documents_generator:
        filepath = filegen.download()
        record_list = process_xml_file(filepath)
        all_records += record_list
        file_count += 1
        
        if file_count == limit:
            break
            
    df = pd.DataFrame(all_records)
    return df

def entry_point_2(dataset, write_flag):
    """
    A function to call into this functionality, useful for
    calling from a notebook. 
    Also used below for the script interface.
    
    parameter: dataset is the dataset returned from Dataset.get(name)
    
    returns a Pandas dataframe
    """

    logging.basicConfig(
        format='%(levelname)s: %(message)s',
        filename=f"log_dq_snooper_section.log",
        force=True, level=logging.WARNING)

    df = process_dataset_of_files(dataset)
   
    if write_flag:
        df.to_csv("dq_ccda_snooper_section.csv", index=False)
    return df

def entry_point(dataset_read, dataset_write, export_flag, write_flag, limit):
    """
    A function to call into this functionality, useful for
    calling from a notebook. 
    Also used below for the script interface.
    
    dataset_name is the name of a dataset as seen in the Data tab in Foundry.
    """

    logging.basicConfig(
        format='%(levelname)s: %(message)s',
        filename=f"log_dq_snooper_section.log",
        force=True, level=logging.WARNING)

    df = process_dataset_of_files_by_name(dataset_read, limit)
    
    if export_flag:
        # Save dataset to HDFS/Spark in Foundry
        dq_ccda_snooper_section = Dataset.get(dataset_write)
        dq_ccda_snooper_section.write_table(df)
        print(f"wrote {dataset_write} dataset")
        
    if write_flag:
        df.to_csv(f"{dataset_write}.csv")
        
def main():
    """
    Main function that parses arguments, processes the XML file(s), writes to CSV, exports to HDFS...
    """
    
    parser = argparse.ArgumentParser(
        prog='CCDA DQ Section Snooper',
        description="Finds attributes for <section> elements in the XML file for use in DQ dashboard",
        epilog='epilog?')
    parser.add_argument('-x', '--export', action=argparse.BooleanOptionalAction, help="export to foundry")
    parser.add_argument('-w', '--write_csv', action=argparse.BooleanOptionalAction, help="export to csv")
    parser.add_argument('-ds', '--dataset_strings', help="dataset of files to parse")
    args = parser.parse_args()

    logging.basicConfig(
        format='%(levelname)s: %(message)s',
        filename=f"log_dq_snooper_section.log",
        force=True, level=logging.WARNING)

    all_vocab_codes = pd.DataFrame()
    if args.dataset_strings:
        entry_point(args.dataset_strings, args.export, args.write_csv)
        
        df = process_dataset_of_files_by_name(args.dataset_strings)
    else:
        print("need to specify an input dataset?")
    print(df.iloc[0])
if __name__ == '__main__':
    main()
