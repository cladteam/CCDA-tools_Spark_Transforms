#!/usr/bin/env python3
"""
    section_snooper - looks for specfic sections driven by metadata,
        and shows any ID, CODE and VALUE elements within them.

    TODO: the list after a path in metadata isn't used currently.
        Ultimately it would replce the hard-coded id, code, value
        and effectiveTime parts
"""

import argparse
from lxml import etree as ET  # https://docs.python.org/3/library/xml.etree.elementtree.html
from xml_ns import ns
from vocab_map_file import oid_map
#from util import spark_util
#from util.vocab_spark import VocabSpark


SECTION_PATH = "./component/structuredBody/component/section"
SECTION_CODE = "./code"
# OBSERVATION_SECTION_CODE = "./code[@code=\"30954-2\"]"

subs_admin_prefix = './entry/act/entryRelationship/substanceAdministration'

section_metadata = {
    '48765-2': {  # ALLERGIES  (Dx of allergy)
        './entry/': [],
        './entry/act/effectiveTime': [],
        './entry/act/entryRelationship/observation': [],
        # './entry/act/entryRelationship/observation/code': [],
        # './entry/act/entryRelationship/observation/value': [],
        # './entry/act/entryRelationship/observation/effectiveTime': [],

        './entry/act/entryRelationship/observation/participant/participantRole/playingEntity': [],

        './entry/act/entryRelationship/observation/entryRelationship/observation': [],
    },
    '46240-8': {  # ENCOUNTERS, HISTORY OF
        "./entry/encounter": ['encounter']
    },

    '11369-6': {  # IMMUNIZATION ***
        "./entry/substanceAdministration/consumable/manufacturedProduct/manufacturedMaterial"
    },

    '10160-0': {  # MEDICATIONS, HISTORY OF ***
         "./entry/substanceAdministration/": [
            "consumable/manufacturedProduct/manufacturedMaterial",
            "performer",
            "entryRelationship/observation",
            "entryRelationship/supply/product/manufacturedProduct/manufacturedMaterial"]
    },

    '10183-2': {  # HOSPITAL DISCHARGE ****
         "./entry/act/": ['id', 'code', 'effectiveTime',
                          'entryRelationship/substanceAdministration'],
         (subs_admin_prefix + "/consumable/manufacturedProduct/manufacturedMaterial"): [],
         (subs_admin_prefix + "/performer/assignedEntity"): [],
         (subs_admin_prefix + "/performer/assignedEntity/representedOrganization"): []
    },

    '47519-4': {  # PROCEDURES, HISTORY OF
        "./entry/procedure": ['procedure']
    },

    '474020-5': {  # FUNCTIONAL STATUS (observations)
        "./entry/observation":  ['observation']
    },

    '30954-2': {  # RESULTS
        "./entry/organizer/component/observation": ['observation']  # may be multiple components
    },

    '8716-3':  {  # VITAL SIGNS
        "./entry/organizer/component/observation": ['observation']  # may be multiple components
    },
}

if __name__ == '__main__':

    parser = argparse.ArgumentParser(
        prog='CCDA - OMOP Code Snooper',
        description="finds all code elements and shows what concepts the represent",
        epilog='epilog?')
    parser.add_argument('-f', '--filename', required=True, help="filename to parse")
    parser.add_argument('level', choices=['top', 'section', 'element', 'detail' ])
    args = parser.parse_args()
    tree = ET.parse(args.filename)

    # SECTION_PATH = "./component/structuredBody/component/section"
    section_elements = tree.findall(SECTION_PATH, ns)
    print("\n\n")

    if args.level == 'section' or args.level == 'element' or args.level == 'detail':
        for section_element in section_elements:

            section_type = ''
            section_code = ''
            section_code_system = ''
            # SECTION_CODE = "./code"
            # section_code_element = section_element.find(SECTION_CODE, ns)   # just a find doesn't work
            for section_code_element in section_element.findall("./code", ns):
                if 'displayName' in section_code_element.attrib:
                    section_type = section_code_element.attrib['displayName']
                if 'code' in section_code_element.attrib:
                    section_code = section_code_element.attrib['code']
                if 'codeSystem' in section_code_element.attrib:
                    section_code_system = section_code_element.attrib['codeSystem']
                if section_type == '':
                    if section_code_system in oid_map:
                        vocab = oid_map[section_code_system][0]
                        #details = VocabSpark.lookup_omop_details(spark, vocab, section_code)
                        #if details is not None:
                        #    section_type = details[2]

            section_template_id = ''
            section_count=0
            for section_template_id_element in section_element.findall("./templateId", ns):
                section_count+=1
                if 'root' in section_template_id_element.attrib:
                    section_template_id = section_template_id_element.attrib['root']


            print(f"SECTION templateId:\"{section_template_id}\" count:{section_count}  type:\"{section_type}\" code:\"{section_code}\" ", end='')
            section_code = section_code_element.attrib['code']
            if section_code is not None and section_code in section_metadata:
                print("")
                if args.level == 'element' or args.level == 'detail':
                    for entity_path in section_metadata[section_code]:
                        print(f"  MD section code: \"{section_code}\" path: \"{entity_path}\" ")
                        if args.level == 'detail':
                            for entity in section_element.findall(entity_path, ns):
                                print((f"    type:\"{section_type}\" code:\"{section_code}\", "
                                       f" tag:{entity.tag} attrib:{entity.attrib}"), end='')

                                # show_effective_time(entity)

                                # referenceRange

                                # show_id(entity)

                                # show_code(entity)

                                # show_value(entity)
                                print("")
            else:
                print(f"No metadata for \"{section_code}\"     ")
