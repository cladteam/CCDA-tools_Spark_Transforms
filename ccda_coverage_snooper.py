#!/usr/bin/env python3
"""
Collects elements tagged as code, value, id, effectiveTime, high, low and birthtime, and more
groups them by section/entry path instance and  logs  it all to a dataset and csv.

"""

import os
import argparse
import re
import lxml.etree as ET
from xml_ns import ns
from vocab_map_file import oid_map
import vocab_maps
import pandas as pd
from collections import defaultdict

ccda_code_values_columns = [ 'filename', 'template_id', 'path', 'field_tag', 'attributes']

ccda_tags = [ 'code', 'codeSystem', 'value', 'id', 'effectiveTime', 'high', 'low', 'birthtime']


def snoop_section(tree, filename):
    trace_df = pd.DataFrame(columns=ccda_code_values_columns)
    section_elements = tree.findall(".//section", ns)
    for section_element in section_elements:

        section_template_id = "n/a"
        section_template_ele = section_element.findall("templateId",ns)
        if len(section_template_ele) > 0:
            section_template_id = section_template_ele[0].get('root')

        entry_elements = section_element.findall("entry", ns)
        for entry_ele in entry_elements:

            for tag in ccda_tags:
                tag_elements = entry_ele.findall(f".//{tag}", ns)
                for element in tag_elements:
                    # get path, clean elements of namespaces and re-assemble
                    raw_element_path = tree.getelementpath(element)
                    element_path = re.sub(r'{.*?}', '', raw_element_path)
                    element_path = "/".join(element_path.split("/"))

                    # copy and clean the attribute dict
                    element_attribs_dict = { re.sub(r'{.*}', '', a):
                                       re.sub(r'{.*}', '', element.attrib[a])
                                       for a in element.attrib  }

                    new_row = pd.DataFrame([{
                        'filename': filename,
                        'template_id': section_template_id,
                        'path': element_path,
                        'field_tag': tag,
                        'attributes': element_attribs_dict
                    }])
                    trace_df = pd.concat([trace_df, new_row], ignore_index=True)
    return(trace_df)


def main():
    parser = argparse.ArgumentParser(
        prog='CCDA - OMOP Code Snooper',
        description="finds all code elements and shows what concepts the represent",
        epilog='epilog?')
    parser.add_argument('-f', '--filename', help="filename to parse")
    parser.add_argument('-d', '--directory', help="directory of files to parse")
    args = parser.parse_args()

    if args.filename is not None:
        tree = ET.parse(args.filename)
        file_df = snoop_section(tree, args.filename)
        #pd.set_option('display.max_rows', len(file_df))
        pd.set_option('display.max_columns', None) 
        pd.set_option('display.width', None) 
        print(file_df)
        file_df.to_csv(f"ccda_coverage_snooper.csv", sep=",", header=True, index=False)
    elif args.directory is not None:
        all_files_df = pd.DataFrame(columns=ccda_code_values_columns)
        only_files = [f for f in os.listdir(args.directory) if os.path.isfile(os.path.join(args.directory, f))]
        for filename in (only_files):
            if filename.endswith(".xml"):
                tree = ET.parse(os.path.join(args.directory, filename))
                file_df = snoop_section(tree, filename)
                all_files_df = pd.concat([all_files_df, file_df], ignore_index=True)
        #pd.set_option('display.max_rows', len(all_files_df))
        pd.set_option('display.max_columns', None) 
        pd.set_option('display.width', None) 
        print(all_files_df)
        all_files_df.to_csv(f"ccda_coverage_snooper.csv", sep=",", header=True, index=False)
    else:
        logger.error("Did args parse let us  down? Have neither a file, nor a directory.")


if __name__ == '__main__':
    main()
