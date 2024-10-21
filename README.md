# CCDA-tools
Tools to investigate the contents of CCDA documents

This is a collection of analysis tools to be used to compare an OMOP
mapping to the output here to help see that the mapping is complete.
The code snoopers are pretty simple. They produce a list of codeSytem OID, code pairs.
The more general snoopers are more experimental.

## Tools
- section_code_snooper.py
    Driven by a list of sections and their template IDs,
       this code looks for codes found in such a section and lists them.

- header_code_snooper.py
    Finds and outputs code elements found under certain header elements


- section_snooper.py
    Looks for specfic sections driven by metadata,
        and shows any ID, CODE and VALUE elements within them.


- header_snooper.py
    Driven by three levels of metadata for top-level header elements,
    middle elements, and attributes, shows what is foudn in the header. Mostly
    involving time, assinged person, assigned entity and encompassing encounter.


- create_map_to_standard.py
    Takes an OID map, the OMOP concept and concept relationship tables and 
    produces a table suitable for mapping from (OID , concept_code) to a 
    standard concept_id.

    The OMOP vocabulary tables  are in the CCDA_OMOP_Private repository.
    The OID map is in the CCDA-data repository.

    The  output has a zillion columns. We need concept_id and domain_id for sure.
    section is useful for considering the domain_id routing issue: what  CCDA sections
    produce data for which OMOP domains.

    TODO: this code so far does not deal with dates or the  invalid_reason column.

## to run
- ./section_code_snooper.py
  - default input  is ../CCDA-data/resources
  - default output is the snooper_input directory
- ./header_code_snooper.py
  - default input  is ../CCDA-data/resources
  - default output is the snooper_input directory
- ./create_map_to_standard.py
  - default input is the snooper_input directory
  - defualt output is uber_map_to_standard.csv
