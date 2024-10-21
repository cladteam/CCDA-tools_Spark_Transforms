#!/usr/bin/env python3

"""
    This script reads CSVs created by  the ccda_coverage_snooper.py and
the conversion process from CCDA_OMOP_by_Python in data_driven_parse.py
(also called by layer_datasets.py), into a duckdb. The snooper creates
ccda_coverage_snooper.csv and the conversion process creates trace.csv

TODO: need to snoop the header as well!
#0                 recordTarget/patientRole/id
#1  recordTarget/patientRole/patient/birthTime

TODO: need to filter the trace for things we won't use.
This also filters the substanceAdministraton. We know we'll ultimtely get that, but not doing so currently.
I'm trying to match the trace and snoop with work as it stands. Still down to jus 25/80! 
awk -F, '{print $3}'  ccda_coverage_snooper.csv | grep -v entryRelationship | grep -v assignedAuthor | grep -v manufacturedProduct | grep -v referenceRange | less | grep -v substanceAdministration | grep -v "observation/id" | grep -v "organizer/effectiveTime" | grep -v "entry/act"


"""
import duckdb
import pandas as pd

conn = duckdb.connect()
#pd.set_option('display.max_columns', 10)
#pd.set_option('display.width', 1000)
pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)
pd.set_option('display.width', None)
pd.set_option('display.max_colwidth', None)



section_oid_map_ddl = "CREATE TABLE section_oid_map (oid varchar(20),name varchar(20) ); "
section_oid_map_insert = """ INSERT INTO section_oid_map
                 SELECT oid,  name
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
        SELECT filename, template_id, root_xpath, element_tag, config_type,
                domain, omop_field_tag, attribute_value, attributes
        FROM read_csv("../CCDA_OMOP_by_Python/trace.csv", delim=',', header=True)
"""

conn.execute(snooper_ddl)
conn.execute(trace_ddl)
conn.execute(section_oid_map_ddl)

#df = conn.sql("SHOW ALL TABLES;").df()
#print(df[['database', 'schema', 'name']])
#print("")


conn.execute(snooper_insert)
conn.execute(trace_insert)
conn.execute(section_oid_map_insert)

############################################################################################################3

df=conn.execute("""SELECT *
                   FROM  section_oid_map
                    """).df()
print("OID map")
print(df)
print("")

# count SNOOPER
df=conn.execute("""SELECT count(*) as row_ct, count(distinct path) as d_ct 
		   FROM snooper """).df()
print("Snooper")
print(df)
print("")
df=conn.execute("""SELECT template_id, count(*) as row_ct, count(distinct path) as d_ct 
		   FROM snooper 
		   GROUP BY template_id""").df()
print("Snooper")
print(df)
print("")

# count TRACE 
df=conn.execute("""SELECT count(*) as row_ct, count(distinct root_xpath) as d_ct 
                   FROM trace 
                   WHERE config_type = 'FIELD' AND attribute_value is not null """).df()
print("Trace")
print(df)
print("")

#################### INNER JOIN ##########

# count JOIN
df=conn.execute("""SELECT   count(*) as row_ct, count(distinct s.path) as path_ct, count(distinct root_xpath) as xpath_ct 
                   FROM snooper s join trace t on  s.path = t.root_xpath
                   WHERE config_type = 'FIELD' AND attribute_value is not null 
		   """).df()
print("Count INNER join ")
print(df)
print("")
df=conn.execute("""SELECT  s.template_id, count(*) as row_ct, count(distinct s.path) as path_ct, count(distinct root_xpath) as xpath_ct 
                   FROM snooper s join trace t on  s.path = t.root_xpath
                   WHERE config_type = 'FIELD' AND attribute_value is not null 
		   GROUP BY s.template_id""").df()
print("Count INNER join ")
print(df)
print("")

# select JOIN
df=conn.execute("""SELECT  distinct s.path as distinct_same
                   FROM snooper s join trace t on  s.path = t.root_xpath
                   WHERE config_type = 'FIELD' AND attribute_value is not null
                   ORDER BY root_xpath """).df()
print("INNER join ")
print(df)
print("")

#################### LEFT JOIN ##########

# select count only left  JOIN
df=conn.execute("""SELECT   count(distinct s.path) as left_behind_d, count(s.path) as left_behind
                   FROM snooper s left join trace t on  s.path = t.root_xpath
                   WHERE root_xpath is null AND config_type = 'FIELD' AND attribute_value is not null
		   """).df()
print("Count LEFT join")
print(df)
print("")

# select count only left  JOIN
df=conn.execute("""SELECT section_oid_map.name, count(distinct t.root_xpath) as numerator, count(distinct s.path) as denominator ,
                          concat(count(distinct t.root_xpath)*100//count(distinct s.path),'%') as fraction
                   FROM snooper s 
                   LEFT JOIN trace t ON  s.path = t.root_xpath
		   LEFT JOIN section_oid_map on s.template_id = section_oid_map.oid
                   WHERE config_type = 'FIELD' OR config_type is null
		   GROUP BY section_oid_map.name""").df()
print("Count LEFT join")
print(df)
print("")


# select only left  JOIN  # too long to show
#df=conn.execute("""SELECT  distinct s.path
#		   FROM snooper s left join trace t on  s.path = t.root_xpath
#                  WHERE root_xpath is null
#                     AND config_type = 'FIELD' AND attribute_value is not null """).df()
#print("LEFT join")
#print(df)
#print("")

#################### RIGHT JOIN ##########
# select count only right  JOIN
df=conn.execute("""SELECT  count(distinct root_xpath) as only_trace_d, count(root_xpath) as only_trace
                   FROM snooper s right join trace t on  s.path = t.root_xpath
                   WHERE s.path is null
                   AND config_type = 'FIELD' 
                   AND attribute_value is not null
""").df()
print("Count RIGHT join")
print(df)
print("")

# select only right  JOIN
df=conn.execute("""SELECT distinct root_xpath
                   FROM snooper s right join trace t on  s.path = t.root_xpath
                   WHERE s.path is null AND config_type = 'FIELD' AND attribute_value is not null
                   ORDER BY root_xpath""").df()
print("RIGHT join")
print("This is stuff in the trace that is NOT in the snoop??")
print(df)
print("")



