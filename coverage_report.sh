#!/usr/bin/env bash

# coverage_report.sh 
#
# Runs the coverage report pipeline, including some reconcile and analysis scripts.
#
# Inputs: resources/*.xml CCDA_OMOP_by_Python/trace.csv
# Outputs: coverate_report.csv, count_vocab_by_section.csv, etc.
#
TRACE_AVAILABLE=False


# Coverage Report, compares simple snooper output to trace log from the conversion
python3 -m coverage_report.ccda_coverage_snooper
# OUTPUTS ccda_coverage_snooper.csv

if TRACE_AVAILABLE:
    python3 -m coverage_report.coverage_report

# ?
python3 -m coverage_report.section_coverage_report

# For JHU, counts codes regardless of section, counts codes regardless of section
python3 vocab_snooper.py
# OUTPUTS vocab_discovered_codes_with_counts.csv
#         vocab_discovered_codes_expanded.csv
#         counts.csv
#
# For JHU analysis, to prioritize working on the vocabs.
# This counts like the above, but groups by section
python3 -m coverage_report.count_vocab_by_section
# OUTPUTS count_vocab_by_section.py 


# Compare vocab_snooper.py's counts.csv  with count_vocab_by_section.csv
# OUTPUT: section_subtotals.csv
python3 -m coverage_report.reconcile_counts
