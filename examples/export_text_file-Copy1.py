""" export a dataset to HDFS or materializ it.
    This does require setting up a sycnh job using
    the datasets control column in Workspaces
    
    - looks like /foundry/exports is a special file system
      ...probably a link to a place outside this VM that
      the synch job can get to to put it into HDFS.
"""
     
from foundry.transforms import Dataset
import pandas as pd
from pyspark.sql import Row


df = spark.createDataFrame([
    Row(a=1, b=2., c='string1', d=date(2000, 1, 1), e=datetime(2000, 1, 1, 12, 0)),
    Row(a=2, b=3., c='string2', d=date(2000, 2, 1), e=datetime(2000, 1, 2, 12, 0)),
    Row(a=4, b=5., c='string3', d=date(2000, 3, 1), e=datetime(2000, 1, 3, 12, 0))
])

df 

#test_output_ds = Dataset.get("test_output_ds")
#test_output_ds.write_table()