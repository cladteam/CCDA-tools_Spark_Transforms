
"""
    vocab_snooper_simple.py

    ** ONLY PRODUCES "vocab_discovered_codes" ***!!
    
    INPUT: 
       - (-d) resources directory of CCDA XML files
       - (-df) dataset of files
       - (-ds) dataset of strings
       - (-f) individual file

    OUTPUT: "vocab_discovered_codes"
        
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
    "codeSystem",         # @codeSystem, vocabulary OID
    "src_cd"             # @code
]

concepts_introduced_in_mapping = [
    {   # pregnancy
        "codeSystem": "2.16.840.1.113883.6.96",        
        "src_cd": "289908002",           
    }
]

def snoop_for_code_tag(tree, expr):
    """
    Finds all elements matching the XPath expression (expr) in the 
    XML tree and extracts relevant attributes.
    Appends the extracted information in a dataframe.
    """
    element_list = tree.xpath(expr)

    src_cd_list=[]   
    codeSystem_list=[]
    
    for element in element_list:

        src_cd = element.get('code')
        if src_cd is not None:
            src_cd = src_cd.strip()
        src_cd_list.append(src_cd)

        codeSystem = element.get('codeSystem')
        codeSystem_list.append(codeSystem)

    return (src_cd_list, codeSystem_list)


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

    (src_cd_list, codeSystem_list ) = snoop_for_code_tag(tree, ".//*[@codeSystem]")


    return (src_cd_list, codeSystem_list ) 


def process_dataset_of_files(dataset_name):
   
    all_codes_list = []
    all_codeSystems_list = []
    ccda_documents = Dataset.get(dataset_name)
    ccda_documents_generator = ccda_documents.files()    
    i=0
    for filegen in ccda_documents_generator:
        i+=1
        if i%60 == 0:
            print(f"file number: {i} {i/60} ")

        filepath = filegen.download()
        (src_code_list, codeSystem_list) = process_xml_file(filepath)
        all_codes_list +=   src_code_list
        all_codeSystems_list += codeSystem_list

    vocab_codes = pd.DataFrame({
        "codeSystem": all_codeSystems_list,
        "src_cd": all_codes_list
        })
    vocab_codes  = add_concepts_introduced_in_mapping(vocab_codes)
        
    return vocab_codes


def export_to_hdfs(codes):
        print("exporting vocab_discovered_codes")
        ds = Dataset.get("vocab_discovered_codes")
        ds.write_table(codes)
        

def code_entry_point_files(dataset_name):
    """ 
    Similar to main() but for calling from code, not command line
    This one is for processing a dataset of files.
    """

    logging.basicConfig(
        format='%(levelname)s: %(message)s',
        filename=f"log_vocab_snooper.log",
        force=True, level=logging.WARNING)

    vocab_discovered_codes = process_dataset_of_files(dataset_name)
    vocab_discovered_codes.drop_duplicates(inplace=True)
    vocab_discovered_codes.to_csv("vocab_discovered_codes.csv")

    export_to_hdfs(vocab_discovered_codes)
    

