from queries import *
from rest_utils import *
from validation import *

def test_rest_query1_na_data_relay():
    test_rest_query1 = make_query1()
    df = execute_query(test_rest_query1, "https://localhost:8447", expected_len=53)
    validate_query1(df)

def test_rest_query1_na_us_data_relay():
    test_rest_query1 = make_query1()
    df = execute_query(test_rest_query1, "https://localhost:8446", expected_len=53)
    validate_query1(df)

def test_rest_query2_na_data_relay():
    test_rest_query2 = make_query2()
    df = execute_query(test_rest_query2, 'https://localhost:8447', expected_len=50003)
    validate_query2(df, 15)

def test_rest_query2_na_us_data_relay():
    test_rest_query2 = make_query2()
    df = execute_query(test_rest_query2, 'https://localhost:8446', expected_len=50003)
    validate_query2(df, 15)


def test_rest_query3_na_data_relay():
    test_rest_query = make_query3()
    r = submit_query(test_rest_query, 'https://localhost:8447')
    expected = 'Relay server error: invalid query: There must be exactly one entity per query.'
    assert(r.text==expected)

def test_rest_query3_na_us_data_relay():
    test_rest_query = make_query3()
    r = submit_query(test_rest_query, 'https://localhost:8447')
    expected = 'Relay server error: invalid query: There must be exactly one entity per query.'
    assert(r.text==expected)

def test_rest_query4_na_data_relay():
    test_rest_query4 = make_query4()
    df = execute_query(test_rest_query4, 'https://localhost:8447', expected_len=53)
    validate_query4(df)

def test_rest_query4_na_us_data_relay():
    test_rest_query4 = make_query4()
    df = execute_query(test_rest_query4, 'https://localhost:8446', expected_len=53)
    validate_query4(df)

def test_rest_tpch_q1_na_data_relay():
    tpch1 = make_tpch_q1_query()
    df = execute_query(tpch1, 'https://localhost:8447', expected_len=11)
    validate_tpch_q1(df)

def test_rest_tpch_q1_na_us_data_relay():
    tpch1 = make_tpch_q1_query()
    df = execute_query(tpch1, 'https://localhost:8446', expected_len=11)
    validate_tpch_q1(df)

def test_rest_tpch_q1_na_data_relay_all_access():
    tpch1 = make_tpch_q1_query()
    df = execute_query(tpch1, 'https://localhost:8447', access_level='all_access', expected_len=22)
    validate_tpch_q1_all_access(df)

def test_rest_tpch_q1_na_us_data_relay_all_access():
    tpch1 = make_tpch_q1_query()
    df = execute_query(tpch1, 'https://localhost:8446', access_level='all_access', expected_len=22)
    validate_tpch_q1_all_access(df)
