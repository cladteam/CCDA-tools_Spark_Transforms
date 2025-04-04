

"""
    Show how to read a file from a dataset.
    The dataset is a dictionary of strings where the keys are filenames
    and values are single strings of the contents. It does happen that
    you get a dataset with just a single key,value pair.

    To do this, you have to import the dataset into Jupyter using the topmost
    icon in the vertical list on the left that looks like a spreadsheet.
    If you click near the filename, a popup appears with a block of code you
    can copy. You can see these two lines below. The first is a call to 
    Dataset.get(), and the second to files().download().

    For completeness and contrast, a file directly available here is also read.
"""

from foundry.transforms import Dataset
ccda_ccd_b1_ambulatory_v2 = Dataset.get("ccda_ccd_b1_ambulatory_v2")
ccda_ccd_b1_ambulatory_v2_files = ccda_ccd_b1_ambulatory_v2.files().download()


path = ccda_ccd_b1_ambulatory_v2_files['CCDA_CCD_b1_Ambulatory_v2.xml']
with open(path) as f:
    for line in f:
        print(line)

