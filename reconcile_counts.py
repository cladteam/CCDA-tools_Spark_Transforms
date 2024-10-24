#!/usr/bin/env python3

import pandas as pd

counts_df = pd.read_csv("counts.csv")
# ,src_cd,codeSystem,oid,counts

section_counts_df = pd.read_csv("count_vocab_by_section.csv")
# ,code,codeSystem,section_name,counts


# Compare Totals
counts_sum  = counts_df['counts'].sum() #axis=1)
print(f"counts_sum {counts_sum}")
section_counts_sum  = section_counts_df['counts'].sum() #axis=1)
print(f"section_counts_sum {section_counts_sum}")

#counts_sum 1862
#section_counts_sum 1183


# Compare by CodeSystem
#codeSystem_counts_df = counts_df.groupby(['codeSystem']).size().reset_index(name='counts').sort_values(by=['counts', 'codeSystem'])
#codeSystem_section_counts_df = section_counts_df.groupby(['codeSystem']).size().reset_index(name='counts').sort_values(by=['counts', 'codeSystem'])
codeSystem_counts_df = counts_df.groupby(['codeSystem']).size().reset_index(name='counts').sort_values(by=['codeSystem'])
codeSystem_section_counts_df = section_counts_df.groupby(['codeSystem']).size().reset_index(name='counts').sort_values(by=['codeSystem'])

codeSystem_counts_df.to_csv("codeSystem_counts.csv")
codeSystem_section_counts_df.to_csv("codeSystem_section_counts.csv")

section_subtotals_df = section_counts_df.groupby(['section_name']).size().reset_index(name='counts').sort_values(by=['counts'])
section_subtotals_df.to_csv("section_subtotals_df.csv")

# While we're here: total of each section, regardless of vocab



