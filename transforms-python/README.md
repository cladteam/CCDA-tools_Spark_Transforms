# Python Transforms in Foundry

## Overview

Python is the most comprehensive language for authoring data transformations from within Foundry. Python Transforms include support for batch and incremental pipelines, creating and sharing reusable code libraries, and defining data expectations to ensure high quality data pipelines.

To get started, open repo://transforms-python/src/myproject/datasets/examples.py and uncomment the example transform.

## Local Development

It is possible to carry out high-speed, iterative development of Python Transforms locally. To get started, click the "Work locally" button in the top right.

## Unit Testing

Unit testing is supported in Python transforms. Tests can be enabled by applying the `com.palantir.transforms.lang.pytest-defaults` Gradle plugin in the Python project repo://transforms-python/build.gradle file.

## Data Expectations

Data Expectations can be set up in a Python transforms repository. To get started, open the library search panel on the left side of your Code Repository, search for `transforms-expectations`, and click on "Add library" within the library tab.

## Nuances
The code basically finds all the code elements it can, and then searches for value elements from their parents. It links them with the XMLpath. The output comes from the list of codes, and when a value is available it is added. If for some reason there are more than a single value available, they are added in addtional rows.
The code includes a clean_path column that doesn't have the indexes in it, so you can group data by locations in the document. This helps us find paths to filter out.

## Rule Filtering
Adam found a list of concepts that appear too often and don't apply. These concepts appear in many places. A questoin
was if they appear in places where we shouldn't be looking for anything. So that's one list of paths and sections where they appear.
https://foundry.cladplatform.org/workspace/vector/view/ri.vector.main.workbook.050e2126-9d8f-4aea-a371-2363c13d1c5d?branch=master

Of course the mapping configurations have paths and sections of things we want. So one way to proceed is to remove
paths and sections from Adam's concepts unless they appear in a mapping configuration. At the level of a path regardless 
of section, almost anything with entryRelationship in it can be removed. There are paths for drug_exposure that have
entryRelationship that should be kept. I think the concept on an organizer of observations is another universal one.

It's important to remember the goal of this exercise is to find relevant concepts that are under paths and/or sections
we haven't considered. It should be a separate task to constrain the section snooper to only those paths we have
used in mapping confiurations. Getting the numbers to line up with a more restrictive set of section_counts like
that would test a slew of other issues, but not having left out an overlooked portion of the document.

Making things more complicated is the question of whether in some cases we should look at a value_as_concept instead
of the code's concept attribute. Some of the concepts in Adam's list suggest value_as_concept might be more appropriate to OMOP.

### TL;DR so far (July 2025)
so far what makes sense is to:
-  filter the list of concepts out
- don't use a concept that is on an organizer of observations
- don't use paths that haven entryRelationship in them except for ones used in drug_exposure


### Proposal: a second set of counts from paths in mapping configurations
Have a second set of counts or CCDA source numbers for the DQ dashboard that only look in 


### based on past concepts to filter on 
- 48767-8  Annotation comment [Interpretation] Narrative	Clinical Observation	Standard	Valid	Measurement
  - Q: WHAT VALUE_AS_CONCEPT comes with these?
    - A: FEW
  - many paths ending in entryRelationship/act
  - .../entry/organizer/component/observation in 2.16.840.1.113883.10.20.22.2.3.1
  - .../entry/organnizer/component/procedure in 2.16.840.1.113883.10.20.22.2.3.1
- 48768-6 Payment sources Document,	Clinical Observation,	Standard,	Valid,	Observation
  - typo??
  - sections
    - 2.16.840.1.113883.10.20.22.2.18
  - paths
    - component/structuredBody/comoponent/section/entry/act
      - OK TO FILTER whole path
  - mapping configurations
    - condition.py  from section ....22.2.5
- 33999-4 Diagnosis status,	Clinical Observation,	Standard,	Valid,	Measurement
  - Q: WHAT VALUE comes with these?
    - A: FEW: few codes (1-3 distinct) for each section where this is used
  - sections
    - 2.16.840.1.113883.10.20.22.2.3.1 Relevant diagnostic tests and/or laboratory data
    - 2.16.840.1.113883.10.20.22.2.1.1, 2.16.840.1.113883.10.20.22.2.1 History of Medication Use
    - 2.16.840.1.113883.10.20.22.2.8 Assessments
    - 2.16.840.1.113883.10.20.22.2.4.1 Vital Signs
    - 2.16.840.1.113883.10.20.22.2.22 History of Encounters
    - 2.16.840.1.113883.10.20.22.2.5.1 Problem list
    - 2.16.840.1.113883.10.20.22.2.2 History of Immunization Narrative
    - 2.16.840.1.113883.10.20.22.2.17 29762-2 null
    - 2.16.840.1.113883.10.20.22.2.6.1 Allergies and adverse reactions Document
    - 2.16.840.1.113883.10.20.22.2.5 Problem list
    - 2.16.840.1.113883.10.20.22.2.20 HISTORY OF PAST ILLNESS
    - 2.16.840.1.113883.10.20.22.2.6 Allergies and adverse reactions Document
    - 2.16.840.1.113883.10.20.22.2.38
    - 2.16.840.1.113883.10.20.22.2.10 Plan of care note
    - 2.16.840.1.113883.10.20.22.2.15 Family History
  - paths - all end in entryRelationship/observation
    - component/structuredBody/component/section/entry/substanceAdministration/entryRelationship/observation
    - component/structuredBody/component/section/entry/observation/entryRelationship/observation
    - component/structuredBody/component/section/entry/organizer/component/observation/entryRelationship/observation
      - observation.py
      - measurement.py
    - component/structuredBody/component/section/entry/act/entryRelationship/observation/entryRelationship/observation 
      - condition.py
    - component/structuredBody/component/section/entry/encounter/entryRelationship/act/entryRelationship/observation/entryRelationship/observation
- 10157-6 History of family member diseases Narrative,	Clinical Observation,	Standard,	Valid,	Observation
  - not present
- 64572001 Disease,	Disorder,	Standard,	Valid,	Condition
  - Q: WHAT VALUE comes with these?
    - A: *MANY*  Even for the one path without entryRelationship, History of Past Illness, 
      section/entry/observation there are 3625 concepts in 14255 rows!
  - sections
    - 2.16.840.1.113883.10.20.22.2.5.1  Problem list .../entry/act/entryRelationship/observation
    - 2.16.840.1.113883.10.20.22.2.5 Problem list .../entry/act/entryRelationship/observation
    - 2.16.840.1.113883.10.20.22.2.20 HISTORY OF PAST ILLNESS in 22.2.20, section/entry/observation (80)
    - 2.16.840.1.113883.10.20.22.2.15 Family History .../entry/organizer/component/observation
  - paths
    - component/structuredBody/component/section/entry/act/entryRelationship/observation 
      - condition.py
    - component/structuredBody/component/section/entry/observation
      - observation_social_history*
      - procedure_activity_observation.py
    - component/structuredBody/component/section/entry/organizer/component/observation
      - measurement_vital_signs.py
      - measurement.py
- 282291009 Diagnosis interpretation,	Observable Entity,	Standard, Valid,	Observation
  - Q: WHAT VALUE comes with these?  50k values!!!
    - A: *MANY*  especially assessments, problem list, history of medication, history of procedures, history of hospitalizations
  - sections
    - 2.16.840.1.113883.10.20.22.2.8 Assessments
    - 2.16.840.1.113883.10.20.22.2.7  History of Procedures Document
    - 2.16.840.1.113883.10.20.22.2.22 History of Encounters
    - 2.16.840.1.113883.10.20.22.2.1 History of Medication Use
    - 2.16.840.1.113883.10.20.22.2.5.1 Problem list
    - 2.16.840.1.113883.10.20.22.2.10 Plan of care note
  - paths  all end in entryRelationship/observation
    - component/structuredBody/component/section/entry/act/entryRelationship/observation 
      - condition.py
    - component/structuredBody/component/section/entry/procedure/entryRelationship/observation
    - component/structuredBody/component/section/entry/encounter/entryRelationship/act/entryRelationship/observation
    - component/structuredBody/component/section/entry/substanceAdministration/entryRelationship/observation
    - component/structuredBody/component/section/entry/encounter/entryRelationship/observation
- 55607006 Problem,	Clinical Finding,	Non-standard,	Valid,	Condition 
  - Q: WHAT VALUE comes with these?  10k values!!!
    - A: *MANY* in problem list (...22.2.5.1) and history of procedures (...22.2.7)
  - sections
    - 2.16.840.1.113883.10.20.22.2.5.1 Problem list
    - 2.16.840.1.113883.10.20.22.2.7  History of Procedures Document

    - 2.16.840.1.113883.10.20.22.2.10 Plan of care note
    - 2.16.840.1.113883.10.20.22.2.1 History of Medication Use
    - 2.16.840.1.113883.10.20.22.2.38
    - 2.16.840.1.113883.10.20.22.2.58
  - paths 
    - component/structuredBody/component/section/entry/act/entryRelationship/observation 
      - condition.py
    - component/structuredBody/component/section/entry/procedure/entryRelationship/observation
    - component/structuredBody/component/section/entry/substanceAdministration/entryRelationship/observation
    - 2.16.840.1.113883.10.20.22.2.15 Family History .../entry/organizer/component/observation
    - 2.16.840.1.113883.10.20.22.2.20 HISTORY OF PAST ILLNESS in 22.2.20, section/entry/observation (80)

### do not filter, or do so with more criteria. Filter out sections not used by these configs.
Interesting task. Filter out paths, but only when the section's template_id is not one that would be liseted below.
Q: does the section snooper include the section id? YES
    - component/structuredBody/component/section/entry/act/entryRelationship/observation 
      - condition.py 2.16.840.1.113883.10.20.22.2.5(.1)
    - component/structuredBody/component/section/entry/observation
      - observation_social_history*  2.16.840.1.113883.10.20.22.2.17   (point one?)
      - procedure_activity_observation.py   2.16.840.1.113883.10.20.22.2.7(.1)
    - component/structuredBody/component/section/entry/organizer/component/observation
      - measurement_vital_signs.py  2.16.840.1.113883.10.20.22.2.4(.1)
      - measurement.py  2.16.840.1.113883.10.20.22.2.3(.1)
      - observation.py 2.16.840.1.113883.10.20.22.2.3(.1)
### Filter out
    - component/structuredBody/component/section/entry/procedure/entryRelationship/observation
    - component/structuredBody/component/section/entry/encounter/entryRelationship/act/entryRelationship/observation
    - component/structuredBody/component/section/entry/substanceAdministration/entryRelationship/observation
    - component/structuredBody/component/section/entry/encounter/entryRelationship/observation
    - component/structuredBody/component/section/entry/procedure/entryRelationship/observation
    - component/structuredBody/component/section/entry/substanceAdministration/entryRelationship/observation
    - component/structuredBody/comoponent/section/entry/act
    - component/structuredBody/component/section/entry/substanceAdministration/entryRelationship/observation
    - component/structuredBody/component/section/entry/observation/entryRelationship/observation
    - component/structuredBody/component/section/entry/encounter/entryRelationship/act/entryRelationship/observation/entryRelationship/observation
    - ? c/sB/c/section/entry/act/entryRelationship/act  22.2.6  has
### others
- skipping codes on organizers, see test_path_filter
