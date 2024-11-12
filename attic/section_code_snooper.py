#!/usr/bin/env python3

"""
    section_code_snooper - driven by a list of sections and their template IDs,
       this code looks for codes found in such a section and lists them.

    - detail goes to a log file in a logs directory
    - summary gets printed to stdout
"""

import argparse
import os
import re
from lxml import etree as ET  # https://docs.python.org/3/library/xml.etree.elementtree.html
from vocab_map_file import oid_map
from xml_ns import ns

subs_admin_prefix = './entry/act/entryRelationship/substanceAdministration'
HEADER="section,oid,concept_code,concept_name\n"

section_metadata = {
    # Sections ...10.20.22.2.[ 1.1, 3.1, 4.1, 7.1, 14, 22, 22.1, 41 ]

    'Encounters' : {
    	'loinc_code' : '46240-8',
        'root' : ["2.16.840.1.113883.10.20.22.2.22.1", "2.16.840.1.113883.10.20.22.2.22"],
        'element' : "./entry/encounter",
	'sub_elements' :  ['encounter']
    },

    'Medications': {
        'loinc_code':'10160-0',
        'root' : [ "2.16.840.1.113883.10.20.22.2.1.1",  "2.16.840.1.113883.10.20.22.2.1"],
        'element' : "./entry/substanceAdministration/",
        'sub_elements' : [
            "consumable/manufacturedProduct/manufacturedMaterial",
            "performer",
            "entryRelationship/observation",
            "entryRelationship/supply/product/manufacturedProduct/manufacturedMaterial"]
    },

    'Hospital Discharge': {
        'loinc_code':'10183-2',
        'root' : ["2.16.840.1.113883.10.20.22.2.41"],
        'element' : "./entry/act/",
        'sub_elements' : ['id', 'code', 'effectiveTime',
                          'entryRelationship/substanceAdministration']
        #(subs_admin_prefix + "/consumable/manufacturedProduct/manufacturedMaterial"): [],
        #(subs_admin_prefix + "/performer/assignedEntity"): [],
        #(subs_admin_prefix + "/performer/assignedEntity/representedOrganization"): []
    },

    'Procedures': {
        'loinc_code' : '47519-4',
	'root' : ["2.16.840.1.113883.10.20.22.2.7.1"],
        'element' : "./entry/procedure",
        'sub_elements' : ['procedure']
    },

    'Results': {
        'loinc_code' : '30954-2',
        'root' : ["2.16.840.1.113883.10.20.22.2.3.1"],
        'element' : "./entry/organizer/component/observation",
        'sub_elements' :  ['observation']
    },
    # (ToDo these observatoins appear in multiple places?

    'Vital Signs':  {
        'loinc_code' : '8716-3',
        'root' : [ "2.16.840.1.113883.10.20.22.2.4.1", "2.16.840.1.113883.10.20.22.2.4" ],
        'element' : "./entry/organizer/component/observation",
        'sub_elements' : ['observation']
    },

    # Sections Out of Scope 2.16.840.1.113883.10.20.22.2.[2, 2.1, 5.1, 6.1, 8, 9, 10, 11, 12 ,14, 15, 17, 18, 21, 45]
    # Sections Out of Scope 2.16.840.1.113883.10.20.22.1.[6]
    # Sections Out of Scope 1.3.6.1.4.1.19376.1.5.3.1.3.1
    #
    # Section Immunizations OoS   'root'="2.16.840.1.113883.10.20.1.6"/>
    # Section Immunizations OoS   'root'="2.16.840.1.113883.10.20.22.2.2.1"/>
    # Section Immunizations OoS   'root'="2.16.840.1.113883.10.20.22.2.2"/>
    #    '11369-6': { "./entry/substanceAdministration/consumable/manufacturedProduct/manufacturedMaterial"
    # Section Problem List OoS?    root="2.16.840.1.113883.10.20.22.2.5.1"/>
    # Section Allergies OoS       'root'="2.16.840.1.113883.10.20.22.2.6.1"/>
    #    '48765-2': { './entry/': [], './entry/act/effectiveTime': [],
    #        './entry/act/entryRelationship/observation': [],
    #        './entry/act/entryRelationship/observation/entryRelationship/observation': [],
    # Section Functional and Cognitive Status  'root'="2.16.840.1.113883.10.20.22.2.14"/>
    #    '474020-5': {  # FUNCTIONAL STATUS (observations)
    #        "./entry/observation":  ['observation']
    # Section Assessments  OoS             root="2.16.840.1.113883.10.20.22.2.8"/>
    # Section ????  OoS                    root="2.16.840.1.113883.10.20.22.2.9"/>
    # Section Care Plan OoS                root="2.16.840.1.113883.10.20.22.2.10"/>
    # Section Discharge Medications OoS?   root="2.16.840.1.113883.10.20.22.2.11.1"/>
    # Section Reason For Visit OoS?        root="2.16.840.1.113883.10.20.22.2.12"/>
    # Section Family History OoS           root="2.16.840.1.113883.10.20.22.2.15"/>
    # Section Social History OsS           root="2.16.840.1.113883.10.20.22.2.17"/>
    # Section Payers OsS                   root="2.16.840.1.113883.10.20.22.2.18"/>
    # Section Reason for Referral OoS      root="1.3.6.1.4.1.19376.1.5.3.1.3.1"/>
    # Section Advanced Directives OoS      root="2.16.840.1.113883.10.20.22.2.21"/>
    # Section Instructions OoS      root="2.16.840.1.113883.10.20.22.2.45"/>
}


def scan_section(base_name, section_name, section_element):
    i=0
    section_name = re.sub(r"\s", "_", section_name)
    output_filename = f"snooper_output/{base_name}_{section_name}_section_codes.csv"
    with  open(output_filename, 'w', encoding="utf-8") as f:
        f.write(HEADER)
        for section_code_element in section_element.findall('.//code', ns):
            i += 1
            display_name=""
            code=""
            codeSystem=""
            codeType=""
            if 'displayName' in section_code_element.attrib:
                display_name = section_code_element.attrib['displayName']
            if 'code' in section_code_element.attrib:
                code = section_code_element.attrib['code']
            if 'codeSystem' in section_code_element.attrib:
                code_system = section_code_element.attrib['codeSystem']
            f.write(f"{section_name.strip()},{code_system.strip()},{code.strip()},\"{display_name.strip()}\"\n")
    return i


def scan_file(filename):
    base_name = os.path.basename(filename)

    tree = ET.parse(filename)
    for section_name, section_details in section_metadata.items():
        total_n=0
        for template_id in section_details['root']:
            section_path = f"./component/structuredBody/component/section/templateId[@root=\"{template_id}\"]/.."
            section_element_list = tree.findall(section_path, ns)
            for section_element in section_element_list:
                n = scan_section(base_name, section_name, section_element)
                total_n += n
        print(f"FILE: {base_name}  SECTION: {section_name} {total_n} ")


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
