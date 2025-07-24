import re

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
