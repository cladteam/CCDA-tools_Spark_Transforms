#!/usr/bin/env python3
"""
    header_code_snooper -  finds and outputs code elements found under certain header elements

"""
import re
import os
import argparse
from lxml import etree as ET  # https://docs.python.org/3/library/xml.etree.elementtree.html
from xml_ns import ns
from vocab_map_file import oid_map

HEADER="section,oid,concept_code,concept_name\n"

header_elements = [
    'recordTarget/patientRole/patient',
    'documentationOf/serviceEvent',
    'documentationOf/serviceEvent/performer/assignedEntity',
    'componentOf/encompassingEncounter'
]


def scan_file(filename):
    out_filename = re.sub(r"\s", "_", os.path.basename(filename) )
    output_filename = f"snooper_output/{out_filename}_header_codes.csv"
    with  open(output_filename, 'w', encoding="utf-8") as f:
        f.write(HEADER)
        tree = ET.parse(filename)
        for element_path in header_elements:
            for element in tree.findall(f"{element_path}//*[@codeSystem]", ns):
                attributes = element.attrib
                f.write(f"{element_path.strip()},{element.attrib['codeSystem'].strip()},{element.attrib['code'].strip()},\n")


if __name__ == '__main__':

    parser = argparse.ArgumentParser(
        prog='CCDA - OMOP Code Snooper',
        description="finds all code elements and shows what concepts the represent",
        epilog='epilog?')
    #group = parser.add_mutually_exclusive_group(required=True)
    group = parser.add_mutually_exclusive_group()
    group.add_argument('-d', '--directory', help="directory of files to parse", default='../CCDA-data/resources')
    group.add_argument('-f', '--filename', help="filename to parse")
    args = parser.parse_args()

    if args.filename is not None:
        scan_file(args.filename)
    elif args.directory is not None:
        only_files = [f for f in os.listdir(args.directory) if os.path.isfile(os.path.join(args.directory, f))]
        for file in (only_files):
            if file.endswith(".xml"):
            	scan_file(os.path.join(args.directory, file))
    else:
        logger.error("Did args parse let us  down? Have neither a file, nor a directory.")

