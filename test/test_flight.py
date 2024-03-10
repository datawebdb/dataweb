from flight_utils import *
from queries import *
from validation import *

def test_flight_query1_na_data_relay():
    test_query1 = make_query1()
    df = execute_query(test_query1, 'localhost', 50055)
    validate_query1(df)
    

def test_flight_query1_na_us_data_relay():
    test_query1 = make_query1()
    df = execute_query(test_query1, 'localhost', 50054)
    validate_query1(df)


def test_flight_query2_na_data_relay():
    test_query2 = make_query2()
    df = execute_query(test_query2, 'localhost', 50055)
    validate_query2(df)
    
def test_flight_query2_na_us_data_relay():
    test_query2 = make_query2()
    df = execute_query(test_query2, 'localhost', 50054)
    validate_query2(df)

def test_flight_query3_na_data_relay():
    test_query3 = make_query3()

    try:
        df = execute_query(test_query3, 'localhost', 50055)
        raise ShouldHaveFailedException
    except ShouldHaveFailedException:
        raise Exception("Query3 should have failed, but passed!")
    except Exception as e:
        expected = "Flight returned invalid argument error, with message: Query validation failed with error invalid query: There must be exactly one entity per query."
        assert str(e)==expected, f'Query3 failed with unexpected error. Expected: {expected}, actual: {str(e)}'

def test_flight_query3_na_us_data_relay():
    test_query3 = make_query3()

    try:
        df = execute_query(test_query3, 'localhost', 50054)
        raise ShouldHaveFailedException
    except ShouldHaveFailedException:
        raise Exception("Query3 should have failed, but passed!")
    except Exception as e:
        expected = "Flight returned invalid argument error, with message: Query validation failed with error invalid query: There must be exactly one entity per query."
        assert str(e)==expected, f'Query3 failed with unexpected error. Expected: {expected}, actual: {str(e)}'


def test_flight_query4_na_data_relay():
    test_query4 = make_query4()
    df = execute_query(test_query4, 'localhost', 50055)
    validate_query4(df)

def test_flight_query4_na_us_data_relay():
    test_query4 = make_query4()
    df = execute_query(test_query4, 'localhost', 50054)
    validate_query4(df)

def test_flight_tpch_q1_na_data_relay():
    tpch1 = make_tpch_q1_query()
    df = execute_query(tpch1, 'localhost', 50055)
    validate_tpch_q1(df)
    
def test_flight_tpch_q1_na_us_data_relay():
    tpch1 = make_tpch_q1_query()
    df = execute_query(tpch1, 'localhost', 50054)
    validate_tpch_q1(df)

def test_flight_tpch_q1_na_us_data_relay_all_access():
    tpch1 = make_tpch_q1_query()
    df = execute_query(tpch1, 'localhost', 50054, 'all_access')
    validate_tpch_q1_all_access(df)
    

def test_flight_tpch_q1_na_data_relay_all_access():
    tpch1 = make_tpch_q1_query()
    df = execute_query(tpch1, 'localhost', 50055, 'all_access')
    validate_tpch_q1_all_access(df)
