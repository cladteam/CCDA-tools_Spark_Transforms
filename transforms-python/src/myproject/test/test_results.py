import sys
from myproject.test.test_result_data import test_ccda_string
from myproject.test.test_result_data import result_ccda_data
from myproject.test.test_util import do_test_section_snooper
from myproject.test.test_util import print_exc_info
from myproject.datasets.dq_ccda_snooper_section import process_xml_file


def test_1():
    try:
        records = process_xml_file("test_file", test_ccda_string)
    except Exception as x:
        print(f"EXCEPTION when running {x}")
        print_exc_info(sys.exc_info())
        assert False

    do_test_section_snooper(records, result_ccda_data, True)


#rd_response_529030608625435153_2.25.296032531060316834054964486720371020921.xml