from myproject.test.the_test_utils import do_test_section_snooper
from myproject.test.the_test_utils import print_exc_info
from myproject.datasets.dq_ccda_snooper_section import process_xml_file
import sys
 

# rd_response_529030608625435153_2.25.296032531060316834054964486720371020921.xm
result_ccda_data= [
   { 
      'document_type': "Continuity of Care Document (CCD)",
      
      'source': "test_file",
      'section': "2.16.840.1.113883.10.20.22.2.3.1",
      'section_code': "30954-2",
      'section_name': "RESULTS",

      'path': "component/structuredBody/component/section/entry/organizer/component[1]/observation",
      'clean_path': "component/structuredBody/component/section/entry/organizer/component/observation",
      'code': "30313-1",
      'codeSystem': "2.16.840.1.113883.6.1",

      'value_type': "PQ",
      'value_unit': "g/dl",
      'value_value': "10.2",
      'value_code': "",
      'value_codeSystem': "",
      'value_text': None,
      "date": "20120810",
      "start_date": None,
      "end_date": None,
      #"date_debug": "{'value': '20120810'}",
      "date_debug":"""<effectiveTime xmlns="urn:hl7-org:v3" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:cda="urn:hl7-org:v3" xmlns:sdtc="urn:hl7-org:sdtc" value="20120810"/>
                           """, 
      "start_date_debug": None,
      "end_date_debug": None,
   },
   {
      'document_type': "Continuity of Care Document (CCD)",
      
      'source': "test_file",
      'section': "2.16.840.1.113883.10.20.22.2.3.1",
      'section_code': "30954-2",
      'section_name': "RESULTS",

      'path': "component/structuredBody/component/section/entry/organizer/component[2]/observation",
      'clean_path': "component/structuredBody/component/section/entry/organizer/component/observation",
      'code': "33765-9",
      'codeSystem': "2.16.840.1.113883.6.1",

      'value_type': "PQ",
      'value_unit': "10+3/ul",
      'value_value': "12.3",
      'value_code': "",
      'value_codeSystem': "",
      'value_text': None,
      "date": "20120810",
      "start_date": None,
      "end_date": None,
      #"date_debug": "{'value': '20120810'}",
      "date_debug": """<effectiveTime xmlns="urn:hl7-org:v3" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:cda="urn:hl7-org:v3" xmlns:sdtc="urn:hl7-org:sdtc" value="20120810"/>
                           """, 
      "start_date_debug": None,
      "end_date_debug": None
   },
   {
      'document_type': "Continuity of Care Document (CCD)",
      
      'source': "test_file",
      'section': "2.16.840.1.113883.10.20.22.2.3.1",
      'section_code': "30954-2",
      'section_name': "RESULTS",

      'path': "component/structuredBody/component/section/entry/organizer/component[3]/observation",
      'clean_path': "component/structuredBody/component/section/entry/organizer/component/observation",
      'code': "26515-7",
      'codeSystem': "2.16.840.1.113883.6.1",

      'value_type': "PQ",
      'value_unit': "10+3/ul",
      'value_value': "123",
      'value_code': "",
      'value_codeSystem': "",
      'value_text': None ,
      "date": "20120810",
      "start_date": None,
      "end_date": None,
      #"date_debug": "{'value': '20120810'}",
      "date_debug": """<effectiveTime xmlns="urn:hl7-org:v3" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:cda="urn:hl7-org:v3" xmlns:sdtc="urn:hl7-org:sdtc" value="20120810"/>
                           """, 
      "start_date_debug": None, 
      "end_date_debug": None

   }
   
]

test_ccda_string = """
<ClinicalDocument xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
 xsi:schemaLocation="urn:hl7-org:v3 ../../../CDA%20R2/cda-schemas-and-samples/infrastructure/cda/CDA.xsd"
 xmlns="urn:hl7-org:v3"
 xmlns:cda="urn:hl7-org:v3"
 xmlns:sdtc="urn:hl7-org:sdtc">

   <!-- CCD document type -->
   <templateId root="2.16.840.1.113883.10.20.22.1.2"/>

   <componentOf>
      <encompassingEncounter>
         <id extension="1" root="2.16.840.1.113883.4.6"/>
         <code
            code="233604007"
            codeSystem="2.16.840.1.113883.6.96"
            codeSystemName="SNOMED-CT"
            displayName="Pnuemonia"/>
         <effectiveTime>
            <low
               value="20120806"/>
            <high
               value="20120813"/>
         </effectiveTime>
      </encompassingEncounter>
   </componentOf>
   
   <component>
      <structuredBody>
         <component>
            <section>
               <templateId
                  root="2.16.840.1.113883.10.20.22.2.3.1"/>
               <!-- Entries Required -->
               <code code="30954-2" codeSystem="2.16.840.1.113883.6.1" codeSystemName="LOINC" displayName="RESULTS"/>
               <title>RESULTS</title>
               <entry
                  typeCode="DRIV">
                  <organizer
                     classCode="BATTERY"
                     moodCode="EVN">
                     <!-- Result organizer template -->
                     <templateId
                        root="2.16.840.1.113883.10.20.22.4.1"/>
                     <id
                        root="7d5a02b0-67a4-11db-bd13-0800200c9a66"/>
                     <code xsi:type="CE"
                        code="43789009"
                        displayName="CBC WO DIFFERENTIAL"
                        codeSystem="2.16.840.1.113883.6.96"
                        codeSystemName="SNOMED CT"/>
                     <statusCode
                        code="completed"/>
                     <component>
                        <observation classCode="OBS" moodCode="EVN">
                           <!-- Result observation template -->
                           <templateId root="2.16.840.1.113883.10.20.22.4.2"/>
                           <id root="107c2dc0-67a5-11db-bd13-0800200c9a66"/>
                           <code xsi:type="CE"
                              code="30313-1"
                              displayName="HGB"
                              codeSystem="2.16.840.1.113883.6.1"
                              codeSystemName="LOINC"> </code>
                           <text> <reference value="#result1"/>HGB 10.2 text value </text>
                           <statusCode code="completed"/>
                           <effectiveTime value="20120810"/>
                           <value xsi:type="PQ" value="10.2" unit="g/dl"/>
                           <interpretationCode code="N" codeSystem="2.16.840.1.113883.5.83"/>
                           <methodCode/>
                           <targetSiteCode/>
                           <author>
                              <time/>
                              <assignedAuthor> <id root="2a620155-9d11-439e-92b3-5d9816ff4de8"/> </assignedAuthor>
                           </author>
                           <referenceRange>
                              <observationRange> <text>M 13-18 g/dl; F 12-16 g/dl</text> </observationRange>
                           </referenceRange>
                        </observation>
                     </component>
                     <component>
                        <observation classCode="OBS" moodCode="EVN">
                           <!-- Result observation template -->
                           <templateId root="2.16.840.1.113883.10.20.22.4.2"/>
                           <id root="107c2dc0-67a5-11db-bd13-0800200c9a66"/>
                           <code xsi:type="CE"
                              code="33765-9"
                              displayName="WBC"
                              codeSystem="2.16.840.1.113883.6.1"
                              codeSystemName="LOINC"> </code>
                           <text> <reference value="#result2"/>WBC 12.3 text value </text>
                           <statusCode code="completed"/>
                           <effectiveTime value="20120810"/>
                           <value xsi:type="PQ" value="12.3" unit="10+3/ul"/>
                           <interpretationCode code="N" codeSystem="2.16.840.1.113883.5.83"/>
                           <methodCode/>
                           <targetSiteCode/>
                           <author>
                              <time/>
                              <assignedAuthor> <id root="2a620154-9d11-439e-92b3-5d9815ff4de8"/> </assignedAuthor>
                           </author>
                           <referenceRange>
                              <observationRange>
                                 <value xsi:type="IVL_PQ">
                                    <low value="4.3" unit="10+3/ul"/>
                                    <high value="10.8" unit="10+3/ul"/>
                                 </value>
                              </observationRange>
                           </referenceRange>
                        </observation>
                     </component>
                     <component>
                        <observation classCode="OBS" moodCode="EVN">
                           <!-- Result observation template -->
                           <templateId root="2.16.840.1.113883.10.20.22.4.2"/>
                           <id root="107c2dc0-67a5-11db-bd13-0800200c9a66"/>
                           <code xsi:type="CE"
                              code="26515-7"
                              displayName="PLT"
                              codeSystem="2.16.840.1.113883.6.1"
                              codeSystemName="LOINC"> </code>
                           <text> <reference value="#result3"/>PLT 123 text value </text>
                           <statusCode code="completed"/>
                           <effectiveTime value="20120810"/>
                           <value xsi:type="PQ" value="123" unit="10+3/ul"/>
                           <interpretationCode code="L" codeSystem="2.16.840.1.113883.5.83"/>
                           <methodCode/>
                           <targetSiteCode/>
                           <author>
                              <time/>
                              <assignedAuthor> <id root="2a620155-9d11-439e-92b3-5d9815ff4de8"/> </assignedAuthor>
                           </author>
                           <referenceRange>
                              <observationRange>
                                 <value xsi:type="IVL_PQ">
                                    <low value="150" unit="10+3/ul"/>
                                    <high value="350" unit="10+3/ul"/>
                                 </value>
                              </observationRange>
                           </referenceRange>
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

def test_panel():
    try:
        records = process_xml_file("test_file", test_ccda_string)

    except Exception as x:
        print(f"EXCEPTION when running {x}")
        print_exc_info(sys.exc_info())
        assert False

    assert do_test_section_snooper(records, result_ccda_data, verbose=False, picky=False)
