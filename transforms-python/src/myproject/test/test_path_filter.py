import sys
from myproject.test.the_test_utils import print_exc_info
from myproject.util import keep_path


test_data = [
    ('component/structuredBody/component/section/code', True),
    ('component/structuredBody/component/section/entry/observation/code', True),
    ('component/structuredBody/component/section/entry/organizer/code', False),  # filter out this one
    ('component/structuredBody/component/section/entry/organizer/code/with/more/fictional/stuff/for/test', True),
    ('component/structuredBody/component/section/entry/organizer/component/observation/code', True),
    ('other/wild/card/stuff/should/pass', True)
]

path_exclusion_list = [ # not parent paths
    r'.*/section/entry/organizer/code$',
    r'.*/referenceRange/observationRange/code$' 
]

def test_path():

   for test_tuple in test_data:
      keep=False
      try:
         #keep = keep_path(test_tuple[0], path_exclusion_list, True)
         keep = keep_path(test_tuple[0], path_exclusion_list)
      except Exception as x:
        print(f"EXCEPTION when running {x}")
        print_exc_info(sys.exc_info())
        assert False
      assert (keep == test_tuple[1])
      


