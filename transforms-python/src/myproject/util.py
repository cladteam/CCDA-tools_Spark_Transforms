import re

# A set of known document-level template OIDs based on the HL7 list.
# This is used to find the specific templateId that defines the document type.
DOC_TYPE_MAP = {
    "2.16.840.1.113883.10.20.22.1.15": "Care Plan",
    "2.16.840.1.113883.10.20.22.1.4": "Consultation Note",
    "2.16.840.1.113883.10.20.22.1.2": "Continuity of Care Document (CCD)",
    "2.IS.840.1.113883.10.20.22.1.5": "Diagnostic Imaging Report",
    "2.16.840.1.113883.10.20.22.1.8": "Discharge Summary",
    "2.16.840.1.113883.10.20.22.1.3": "History and Physical",
    "2.16.840.1.113883.10.20.22.1.7": "Operative Note",
    "2.16.840.1.113883.10.20.22.1.6": "Procedure Note",
    "2.16.840.1.113883.10.20.22.1.9": "Progress Note",
    "2.16.840.1.113883.10.20.22.1.14": "Referral Note",
    "2.16.840.1.113883.10.20.22.1.13": "Transfer Summary",
    "2.16.840.1.113883.10.20.22.1.10": "Unstructured Document"
}

clean_path_regex = r"\[.+?\]"
def clean_path(path):
    """ Changes an indexed path to a plain path.
        Ex. input: /x/foo[1]/bar[3]/baz[2]
           output: /x/foo/bar/baz
    """
    return re.sub(clean_path_regex, '', path)


def keep_path(path, path_exclusion_list, verbose=False):
    """ Tells whether a string is not excluded by any of the
        patterns in the exclusion list.
        Used to filter out paths in the dq_ccda_section_snooper.
        Should be a clean_path() in that context, that is the rules
        are for paths that don't have the indexes in square brackets.
        See the function above. 

        Q: how to modify this with a template id that makes an exception?
    """
    keep = True
    for exclusion_pattern in path_exclusion_list:
        if re.match(exclusion_pattern, path):
            if verbose:
                print(f"FAiLING {exclusion_pattern} {path}")
            keep = False
    if keep and verbose:
       print(f"\nPASSING pattern:{exclusion_pattern} path:{path}\n")
    return keep
