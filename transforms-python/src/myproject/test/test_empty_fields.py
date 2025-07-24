import sys
from myproject.test.the_test_utils import do_test_section_snooper
from myproject.test.the_test_utils import print_exc_info
from myproject.datasets.dq_ccda_snooper_section import process_xml_file


result_ccda_data= [ ]


test_ccda_string = """
<ClinicalDocument xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
 xsi:schemaLocation="urn:hl7-org:v3 ../../../CDA%20R2/cda-schemas-and-samples/infrastructure/cda/CDA.xsd"
 xmlns="urn:hl7-org:v3"
 xmlns:cda="urn:hl7-org:v3"
 xmlns:sdtc="urn:hl7-org:sdtc">
   
   <component>
      <structuredBody>
         <component>
            <section>
               <templateId
                  root="2.16.840.1.113883.10.20.22.2.3.1"/>
               <!-- Entries Required -->
               <code code="30954-2" codeSystem="2.16.840.1.113883.6.1" codeSystemName="LOINC" displayName="RESULTS"/>
               <title>RESULTS</title>
               <entry typeCode="DRIV">
                  <organizer classCode="BATTERY" moodCode="EVN">
                     <!-- Result organizer template -->
                     <templateId root="2.16.840.1.113883.10.20.22.4.1"/>
                     <id root="7d5a02b0-67a4-11db-bd13-0800200c9a66"/>
                     <code xsi:type="CE"
                        code="43789009"
                        displayName="CBC WO DIFFERENTIAL"
                        codeSystem="2.16.840.1.113883.6.96"
                        codeSystemName="SNOMED CT"/>
                     <statusCode code="completed"/>
                     <component>
                        <observation classCode="OBS" moodCode="EVN">
                           <!-- Result observation template -->
                           <templateId root="2.16.840.1.113883.10.20.22.4.2"/>
                           <id root="107c2dc0-67a5-11db-bd13-0800200c9a66"/>
                           <code />
                           <text> testing for missing attribute robustness</text>
                           <statusCode code="completed"/>
                           <effectiveTime value="20120810"/>
                           <value />
                        </observation>
                     </component>
                  </organizer>
               </entry>
            </section>
         </component>
      </structuredBody>
   </component>
</ClinicalDocument>
"""


def test_empty_fields():
    try:
        records = process_xml_file("test_file", test_ccda_string)
    except Exception as x:
        print(f"EXCEPTION when running {x}")
        print_exc_info(sys.exc_info())
        assert False

    assert do_test_section_snooper(records, result_ccda_data, True)

