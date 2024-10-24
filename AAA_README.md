# CCDA-tools
Tools to investigate the contents of CCDA documents

This is a collection of analysis tools to be used to compare an OMOP
mapping to the output here to help see that the mapping is complete.
The code snoopers are pretty simple. They produce a list of 
codeSytem and OID code pairs.
The more general snoopers are more experimental.

## FLOW
    Resources/*.xml
    |  |
    |  +-->ccda_coverage_snooper.py 
    |       |
    |       +---> ccda_coverage_snooper.csv
    |              |  |
    |              |  +-->count_vocab_by_section.py --> count_vocab_by_section.csv
    |              +--> section_coverage_report.py --> section_coverage_report.csv
    |
    +---> vocab_snooper.py
          |  | \
          |  | +--> counts.csv
          |  +--> (DS) vocab_discovered_codes_with_counts
          +-----> (DS) vocab_discovered_codes_expanded
          
          
## Snoopers
- raw_section_snooper.py
  Working towards data for pre-OMOP side DQ.
  Today, 2024-10-14, it doesn't build a table but prints data from 
  different XML elements by CCDA Section.
  - Templates and attributes, mostly OIDs and dates
  - Entry elements, marked "ENTRY", and not much else??
    Sometimes you see data split into two entries. Need to look closer
    as to why and what useful information mibht be at this level.
  - Code elements within the entry, marked "ENTRY-CODE", with and OID,
    vocabulary name, and code.
  - Value elements also within the entry, marked "ENTRY-VALUE",
    with attributes type, value and unit.
    
  TODO ideas: change the output to a table that records the Code Value elements
  with some idea of where in the doc. they came from: doc and section. Include
  entry if there's anything useful there.
  Consider comparing codes discovered here with those discovered by the less
  structured vocab_snooper.
  For development, we should be able to reconcile the OMOP counts with 
  what comes out here.
  
  
- vocab_snooper.py 
  Creates files for JHU vocabulary crosswalk table creation
  - vocab_discovered_codes.csv and vocab_discovered_codes_expanded.csv.
  It basically looks for elements with a codeSystem attribute and reports
  that along with a code it finds there. The expanded version kkkj

## Other tools and files

- vocab_map_file.py DEPRECATED,to be deleted.
  An early hack with OIDs and concepts map.
- vocab_maps.py
    Utility functions for reading different vocabular files including:
  - oid.csv
    A list of OIDs and related vocabularies
  - omop_concept_files (CONCEPT.csv?)
  - CONCEPT_RELATIONSHIP.csv
    The OMOP table that relates concepts in many ways.
  - SOURCE_TO_CONCEPT_MAP.csv 
    The rarely used OMOP table, often empty.
  
- xml_ns.py
    A short python file with a dictionary of namespace abbreviations
    for passing to the lxml functions.

## Deprecated Snooper
Don't delete these just yet. Their may be some value yet to be mined from them.

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

    TODO: this code so far doesn't deal with dates or the  invalid_reason column.

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
