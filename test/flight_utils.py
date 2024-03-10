import pyarrow as pa
import pyarrow.flight
from pyarrow.flight import FlightClient
import json
import pandas as pd

class ShouldHaveFailedException(Exception):
    pass

def read_certs(access_level) -> (bytes, bytes, bytes):
    """
    Get client cert, key, and cacert as bytes
    """
    with open(f"client_cert_{access_level}.pem", 'rb') as f:
        cert = f.read()

    with open(f"client_key_{access_level}.pem", 'rb') as f:
        key = f.read()

    with open("cacert.pem", 'rb') as f:
        cacert = f.read()

    return (cert, key , cacert) 


def read_endpoint(endpoint, cert, key, cacert):
    ticket = endpoint.ticket
    uri = endpoint.locations[0].uri.decode("utf-8").replace("https", "grpc+tls")
    client: FlightClient = pa.flight.connect(
        uri,
        tls_root_certs=cacert,
        cert_chain=cert,
        private_key=key,
    )
    resp = client.do_get(ticket)
    df = resp.read_pandas()
    source_id = json.loads(ticket.ticket.decode('utf8'))["data_source_id"]
    df["_source_relay_uri_"] = uri
    df["_source_id_"] = source_id
    return df

def execute_query(query, relay_host, relay_port, access_level='default_access') -> pd.DataFrame:
    """
    Executes a query via a 2 step process. First, the query template
    is submitted to the home relay, identified by the relay_host and
    relay_port parameters, as a get_flight_info request. The home relay
    will propagate the request through the network to identify all endpoints
    with relevant data. In step 2, the client will run a do_get call against
    all returned endpoints, concatenating all of the data into a single 
    Pandas DataFrame.
    """
    cert, key, cacert = read_certs(access_level)
    client: FlightClient = pa.flight.connect(
        pa.flight.Location.for_grpc_tls(relay_host, relay_port),
        tls_root_certs=cacert,
        cert_chain=cert,
        private_key=key,
        )
    
    query = json.dumps(query)

    flight_desc = pa.flight.FlightDescriptor.for_command(query)

    resp = client.get_flight_info(flight_desc)
    dfs = []
    for endpoint in resp.endpoints:
        try:
           dfs.append(read_endpoint(endpoint, cert, key, cacert))
        except Exception as e:
           print(e)
    
    df = pd.concat(dfs)
    
    return df