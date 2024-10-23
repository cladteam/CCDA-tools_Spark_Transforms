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
import coverage_report_util as cru

conn = duckdb.connect()
#pd.set_option('display.max_columns', 10)
#pd.set_option('display.width', 1000)
pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)
pd.set_option('display.width', None)
pd.set_option('display.max_colwidth', None)



conn.execute(cru.snooper_ddl)
conn.execute(cru.trace_ddl)
conn.execute(cru.section_oid_map_ddl)

#df = conn.sql("SHOW ALL TABLES;").df()
#print(df[['database', 'schema', 'name']])
#print("")


conn.execute(cru.snooper_insert)
conn.execute(cru.trace_insert)
conn.execute(cru.section_oid_map_insert)


df=conn.execute("""SELECT section_oid_map.name, count(distinct t.root_xpath) as num, count(distinct s.path) as denom,
                          concat(count(distinct t.root_xpath)*100//count(distinct s.path),'%') as fraction,
                          section_oid_map.priority
                   FROM snooper s 
                   LEFT JOIN trace t ON  s.path = t.root_xpath 
                     AND s.template_id = t.template_id
		           LEFT JOIN section_oid_map on s.template_id = section_oid_map.oid
                   WHERE config_type = 'FIELD' OR config_type is null
                    AND  PRIORITY < 100
		           GROUP BY section_oid_map.name, section_oid_map.priority
                   ORDER BY priority, section_oid_map.name
            """).df()
print("Count LEFT join")
print(df)
print("")

df=conn.execute("""SELECT s.template_id, section_oid_map.name, count(distinct t.root_xpath) as num, count(distinct s.path) as denom,
                          concat(count(distinct t.root_xpath)*100//count(distinct s.path),'%') as fraction,
                          section_oid_map.priority
                   FROM snooper s 
                   LEFT JOIN trace t ON  s.path = t.root_xpath
                     AND s.template_id = t.template_id
		           LEFT JOIN section_oid_map on s.template_id = section_oid_map.oid
                   WHERE config_type = 'FIELD' OR config_type is null
              --      AND  PRIORITY < 100
		           GROUP BY section_oid_map.name, s.template_id , section_oid_map.priority
                   ORDER BY priority, section_oid_map.name
            """).df()
print("Count LEFT join")
print(df)
print("")

df=conn.execute("""SELECT s.template_id, section_oid_map.name, t.config_path
                   FROM snooper s 
                   LEFT JOIN trace t ON  s.path = t.root_xpath
                     AND s.template_id = t.template_id
		           LEFT JOIN section_oid_map on s.template_id = section_oid_map.oid
                   WHERE config_type = 'FIELD' 
                      -- OR config_type is null
                     AND  config_path is not null
		           GROUP BY section_oid_map.name, s.template_id , section_oid_map.priority, config_path
                   ORDER BY s.template_id
            """).df()
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


# select only right  JOIN
df=conn.execute("""SELECT distinct root_xpath
                   FROM snooper s right join trace t on  s.path = t.root_xpath
                   WHERE s.path is null AND config_type = 'FIELD' AND attribute_value is not null
                   ORDER BY root_xpath""").df()
print("RIGHT join")
print("This is stuff in the trace that is NOT in the snoop??")
print(df)
print("")



