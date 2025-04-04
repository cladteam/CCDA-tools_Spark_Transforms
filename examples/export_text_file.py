""" write a simple file to the VM filesystem, then
    export to HDFS or materialize.
    This does require setting up a sycnh job using
    the datasets control column in Workspaces

    - looks like /foundry/exports is a special file system
      ...probably a link to a place outside this VM that
      the synch job can get to to put it into HDFS.
"""

from foundry.transforms import Dataset

with open("/foundry/outputs/foobar.txt",'w') as g:
    g.write("foobar")


# for plain text
text_output = Dataset.get("text_output")
text_output.upload_file("/foundry/outputs/foobar.txt")
#text_output.upload_directory("/foundry/outputs/foobar.txt")
