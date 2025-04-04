"""
    dq_ccda_snooper_people.py
    
    Snoops a dataset of CCDA XML files for people info used in DQ.
    
    Output: dq_ccda_snooper_people dataset
"""    

import os
import pandas as pd
import argparse
import logging
import re
import lxml.etree as ET
from xml_ns import ns
import os
import vocab_maps
from collections import defaultdict
from foundry.transforms import Dataset

logger = logging.getLogger(__name__)



def process_xml_file(file_path):
    """ returns a list of dictionaries/records of attributes from 
        persons in the file
    """
    
    data_records = []
    with open(file_path, 'rb') as file:
        tree = ET.parse(file)
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

        elements = [
            ('administrativeGenderCode', 'Gender'),
            ('raceCode', 'Race'),
            ('ethnicGroupCode', 'Ethnicity'),
            ('birthTime', 'BirthTime')
        ]

        # Extract data for each element
        record = {'Filename': os.path.basename(file_path)}
        for tag, label in elements:
            element = tree.find(f'.//hl7:{tag}', namespaces=ns)
            value = element.get('displayName') if tag != 'birthTime' and element is not None else (
                element.get('value') if element is not None else 'Unknown'
            )
            record[label] = value
    
            data_records.append(record)
    
        return data_records



def process_dataset_of_files(dataset, limit):
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
        
        if i == limit:
            break
            
    df = pd.DataFrame(all_records)
    return df

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

def entry_point_2(dataset, export_flag, write_flag, limit=0):
    """
    A function to call into this functionality, usefull for
    calling from a notebook. 
    Also used below for the script interface.
    
    parameter: dataset is the dataset returned from Dataset.get(name)
    
    returns a Pandas dataframe
    """

    logging.basicConfig(
        format='%(levelname)s: %(message)s',
        filename=f"log_dq_snooper_people.log",
        force=True, level=logging.WARNING)

    df = process_dataset_of_files(dataset, limit)
        
    if export_flag:
        # Save dataset to HDFS/Spark in Foundry
        dq_ccda_snooper_people = Dataset.get("dq_ccda_snooper_people")
        dq_ccda_snooper_people.write_table(df)
        print(f"wrote dq_ccda_snooper_people dataset")
        
    if write_flag:
        df.to_csv("dq_ccda_snooper_people.csv")

    return df


def entry_point(dataset_name, export_flag, write_flag):
    """
    A function to call into this functionality, usefull for
    calling from a notebook. 
    Also used below for the script interface.
    
    dataset_name is the name of a dataset as seen in the Data tab in Foundry.
    """

    logging.basicConfig(
        format='%(levelname)s: %(message)s',
        filename=f"log_dq_snooper_people.log",
        force=True, level=logging.WARNING)

    df = process_dataset_of_files_by_name("ccda_response_files")

    if export_flag:
        # Save dataset to HDFS/Spark in Foundry
        dq_ccda_snooper_people = Dataset.get("dq_ccda_snooper_people")
        dq_ccda_snooper_people.write_table(df)
        print(f"wrote dq_ccda_snooper_people dataset")
        
    if write_flag:
        df.to_csv("dq_ccda_snooper_people.csv")



def main():
    """
    Main function that parses arguments, processes the XML file(s), writes to CSV, exports to HDFS...
    """
    
    parser = argparse.ArgumentParser(
        prog='CCDA DQ People Snooper',
        description="Finds attributes for persons in the XML file for use in DQ dashboard",
        epilog='epilog?')
    parser.add_argument('-x', '--export', action=argparse.BooleanOptionalAction, help="export to foundry")
    parser.add_argument('-w', '--write_csv', action=argparse.BooleanOptionalAction, help="export to csv")
    parser.add_argument('-ds', '--dataset_strings', help="dataset of files to parse")
    args = parser.parse_args()

    logging.basicConfig(
        format='%(levelname)s: %(message)s',
        filename=f"log_dq_snooper_people.log",
        force=True, level=logging.WARNING)


    all_vocab_codes = pd.DataFrame()

    if args.dataset_strings:
        entry_point(args.dataset_strings, args.export, args.write_csv)
        
        df = process_dataset_of_strings(args.dataset_strings)
    else:
        print("need to specify an input dataset?")

if __name__ == '__main__':
    main()

