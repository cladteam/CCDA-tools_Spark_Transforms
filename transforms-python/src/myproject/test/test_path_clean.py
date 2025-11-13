import sys
from myproject.test.the_test_utils import print_exc_info
from myproject.util import clean_path




data = [
    ('component/structuredBody/component/section[1]/code',
    'component/structuredBody/component/section/code'),

    ('component/structuredBody/component[3]/section[1]/code[2]',
    'component/structuredBody/component/section/code'),

    ('component/structuredBody/component[3]/section/entry[3]/observation[4]/code',
    'component/structuredBody/component/section/entry/observation/code'),

    ('component/structuredBody/component[9]/section[1]/entry[3]/observation[4]/code[3]',
    'component/structuredBody/component/section/entry/observation/code'),

    ('component/structuredBody/component[9]/section/entry[3]/organizer/code',
    'component/structuredBody/component/section/entry/organizer/code'),

    ('component/structuredBody/component[3]/section/entry[3]/organizer/code/with/more/fictional/stuff/for/test',
    'component/structuredBody/component/section/entry/organizer/code/with/more/fictional/stuff/for/test'),

    ('component/structuredBody/component[3]/section/entry[1]/organizer/component[1]/observation/code',
    'component/structuredBody/component/section/entry/organizer/component/observation/code'),

    ('component/structuredBody/component[3]/section/entry[1]/organizer/component[1]/observation[3]/code',
    'component/structuredBody/component/section/entry/organizer/component/observation/code'),

    ('other[1]/wild[2]/card[3]/stuff[4]/should[4]/pass[5]',
    'other/wild/card/stuff/should/pass'),

    ('other["a"]/wild[\'b\']/card["c"]/stuff["\\#"]/should["$"]/pass["aadc"]',
    'other/wild/card/stuff/should/pass'),

    ('other/wild/card/stuff/should/pass',
    'other/wild/card/stuff/should/pass'),

    ('component/structuredBody/component[1]/section/entry[1]/act', 
     'component/structuredBody/component/section/entry/act')
]


def test_path():
    print("")
    for test_tuple in data:
        cleaned=''
        try:
            cleaned = clean_path(test_tuple[0])
        except Exception as x:
            print(f"EXCEPTION when running {x}")
            print_exc_info(sys.exc_info())
            assert False
        print(f" {cleaned == test_tuple[1]}  {test_tuple[0]} \n"
              f"    expecting: {test_tuple[1]}\n"
              f"    got:       {cleaned} ")
        assert (cleaned == test_tuple[1])
