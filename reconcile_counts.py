#!/usr/bin/env python3

import pandas as pd




# Read and Compare Totals
counts_df = pd.read_csv("counts.csv")
# ,src_cd,codeSystem,oid,counts
counts_sum  = counts_df['counts'].sum()
print(f"counts_sum {counts_sum}")

section_counts_df = pd.read_csv("count_vocab_by_section.csv")
# ,code,codeSystem,section_name,counts
section_counts_sum  = section_counts_df['counts'].sum() 
print(f"section_counts_sum {section_counts_sum}")

#counts_sum 1862
#section_counts_sum 1183 --> 1511


# Compare by CodeSystem
#codeSystem_counts_df = counts_df.groupby(['codeSystem']).size().reset_index(name='counts').sort_values(by=['counts', 'codeSystem'])
#codeSystem_section_counts_df = section_counts_df.groupby(['codeSystem']).size().reset_index(name='counts').sort_values(by=['counts', 'codeSystem'])
codeSystem_counts_df = counts_df.groupby(['codeSystem']).size().reset_index(name='counts').sort_values(by=['codeSystem'])
codeSystem_section_counts_df = section_counts_df.groupby(['codeSystem']).size().reset_index(name='counts').sort_values(by=['codeSystem'])

codeSystem_counts_df.to_csv("codeSystem_counts.csv")
codeSystem_section_counts_df.to_csv("codeSystem_section_counts.csv")

section_subtotals_df = section_counts_df.groupby(['section_name']).size().reset_index(name='counts').sort_values(by=['counts'])
section_subtotals_df.to_csv("section_subtotals_df.csv")

vocab_subtotals_from_counts_df = counts_df.groupby(['codeSystem']).size().reset_index(name='counts').sort_values(by=['counts'])
vocab_subtotals_from_counts_df.to_csv("vocab_subtotals_from_counts_df.csv")
vocab_subtotals_from_sections_df = section_counts_df.groupby(['codeSystem']).size().reset_index(name='counts').sort_values(by=['counts'])
vocab_subtotals_from_sections_df.to_csv("vocab_subtotals_from_sections_df.csv")

# While we're here: total of each section, regardless of vocab



