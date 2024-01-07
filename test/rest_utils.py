import requests
import json
import time
import pandas as pd

def retrieve_query(query_id, host, access_level='default_access'):
    cert = (f"client_cert_{access_level}.pem", f"client_key_{access_level}.pem")
    cacert = "cacert.pem"
    resp = requests.get(
        f'{host}/query/{query_id}',
        cert=cert,
        verify=cacert
    )

    jsonl = []
    for line in resp.iter_lines():
        jsonl.append(json.loads(line))

    return jsonl

def submit_query(query, host, access_level='default_access'):
    cert = (f"client_cert_{access_level}.pem", f"client_key_{access_level}.pem")
    cacert = "cacert.pem"
    resp = requests.post(
        f'{host}/query',
        cert=cert,
        verify=cacert,
        json=query
    )
    return resp

def execute_query(query, host, access_level='default_access', expected_len=2):
    r = submit_query(query, host, access_level=access_level)
    assert(r.status_code==200), f'Submitting query failed with error {r.text}'
    query_id = r.json()['id']
    attempts = 10
    while attempts>0:
        time.sleep(3)
        data = retrieve_query(query_id, host, access_level=access_level)
        try:    
            #If query results are not ready yet, this error is hit
            assert len(data)>=expected_len, f'expected JSONL data stream, got {data}'
            break
        except Exception as e:
            attempts-=1
    
    df = pd.json_normalize(data)
    df['_source_relay_uri_'] = df['_relay_metadata_._source_relay_']
    df['_source_id_'] = df['_relay_metadata_._source_id_']
    df.drop(['_relay_metadata_._source_relay_', '_relay_metadata_._source_id_'], inplace=True, axis=1)
    return df