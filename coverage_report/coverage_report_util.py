#!/usr/bin/env python3

"""
Utils to load tables used in coverage_report
"""

section_oid_map_ddl = "CREATE TABLE section_oid_map (oid varchar(20),name varchar(20), priority integer ); "
section_oid_map_insert = """ INSERT INTO section_oid_map
                 SELECT oid,  name, priority
		 FROM read_csv('section_oid.csv', delim=',', header=True)
"""

snooper_fields = ['filename', 'template_id', 'path', 'field_tag', 'attributes']
snooper_ddl = """
    CREATE TABLE snooper (
        filename varchar(200),
        template_id varchar(20),
        path varchar(250),
        field_tag varchar(20),
        attributes varchar(200) );
"""


snooper_insert = """
        INSERT INTO snooper
        SELECT filename, template_id, path, field_tag, attributes
        FROM read_csv("ccda_coverage_snooper.csv", delim=',', header=True)
"""

trace_fields = ['filename', 'template_id', 'root_xpath', 'element_tag', 'config_type',
                'domain', 'omop_field_tag', 'attribute_value', 'attributes' ]
trace_ddl = """
    CREATE TABLE trace (
        filename varchar(200),
        template_id varchar(20),
        config_path varchar(250),
        root_xpath varchar(250),
        element_tag varchar(20),
        config_type varchar(20),
        domain varchar(20),
        omop_field_tag varchar(20),
        attribute_value varchar(20),
        attributes varchar(200) );
"""

trace_insert = """
        INSERT INTO  trace
        SELECT filename, template_id, config_path,root_xpath, element_tag, config_type,
                domain, omop_field_tag, attribute_value, attributes
        FROM read_csv("../CCDA_OMOP_by_Python/trace.csv", delim=',', header=True)
"""
