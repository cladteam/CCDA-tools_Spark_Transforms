""" export a (Pandas dataframe) dataset to HDFS or materializ it.
    This does NOT(?) require setting up a sycnh job using
    the datasets control column in Workspaces. Though the widgets
    there will help write the 2 lines that convert to a Dataset
    and write it out.

    - looks like /foundry/exports is a special file system
      ...probably a link to a place outside this VM that
      the synch job can get to to put it into HDFS.
"""
from datetime import date
from foundry.transforms import Dataset
import pandas as pd

pandas_df = pd.DataFrame({
    'a': [1,2,3],
    'b': [2.1, 3.2, 4.3],
    'c': [date(2001,1,1), date(2001, 9, 11), date(1776, 7, 4)]
})

test_output_ds = Dataset.get("test_output_ds")
test_output_ds.write_table(pandas_df)
