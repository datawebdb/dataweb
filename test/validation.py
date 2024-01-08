

def validate_query1(df):
    assert len(df)==53, f'Query1 fail: expected 53 records found {len(df)}'
    n_relays = df['_source_relay_uri_'].nunique()
    n_sources = df['_source_id_'].nunique()
    assert n_relays==5, f'Query1 fail: expected data from 5 relays found {n_relays}'
    assert n_sources==6, f'Query1 fail: expected data from 6 sources found {n_sources}'
    assert len(df.columns)==4, f'Query1 fail: expected 4 columns found {len(df.columns)}'

def validate_query2(df):
    assert len(df)==50003, f'Query2 fail: expected 50003 records found {len(df)}'
    n_relays = df['_source_relay_uri_'].nunique()
    n_sources = df['_source_id_'].nunique()
    assert n_relays==5, f'Query2 fail: expected data from 5 relays found {n_relays}'
    assert n_sources==6, f'Query2 fail: expected data from 6 sources found {n_sources}'
    assert len(df.columns)==22, f'Query2 fail: expected 21 columns found {len(df.columns)}'

def validate_query4(df):
    assert len(df)==53, f'Query4 fail: expected 53 records found {len(df)}'
    n_relays = df['_source_relay_uri_'].nunique()
    n_sources = df['_source_id_'].nunique()
    assert n_relays==5, f'Query4 fail: expected data from 5 relays found {n_relays}'
    assert n_sources==6, f'Query4 fail: expected data from 6 sources found {n_sources}'
    assert len(df.columns)==4, f'Query4 fail: expected 4 columns found {len(df.columns)}'

def validate_tpch_q1(df):
    assert len(df)==11, f'Tpch Q1 Fail: Expected 11 rows for tpch query 1, got {len(df)}'
    assert len(df.columns)==12, f'Tpch Q1 Fail: Expected 12 columns for tpch query 1, got {len(df.columns)}'
    n_relays = df['_source_relay_uri_'].nunique()
    n_sources = df['_source_id_'].nunique()
    assert n_relays==5, f'Tpch Q1 Fail: expected data from 5 relays found {n_relays}'
    assert n_sources==6, f'Tpch Q1 Fail: expected data from 6 sources found {n_sources}'

    global_order_count = df.count_order.sum()
    assert global_order_count==147648, f'Expected global order count of 147648, got {global_order_count}'

    global_quantity_count = (df.avg_qty * df.count_order).sum()
    global_weighted_avg_qty = global_quantity_count / global_order_count

    assert global_weighted_avg_qty==25.4589361183355, f'Expected global_weighted_avg_qty of 25.4589361183355, got {global_weighted_avg_qty}'

def validate_tpch_q1_all_access(df):
    newrows = len(df[df.returnflag!='N'])
    assert newrows==11, f'All access user expected 11 rows where returnflag!="N" but found {newrows}'
    assert len(df)==22, f'Tpch Q1 Fail: Expected 22 rows for tpch query 1 all access, got {len(df)}'
    assert len(df.columns)==12, f'Tpch Q1 Fail: Expected 12 columns for tpch query 1, got {len(df.columns)}'
    n_relays = df['_source_relay_uri_'].nunique()
    n_sources = df['_source_id_'].nunique()
    assert n_relays==5, f'Tpch Q1 Fail: expected data from 5 relays found {n_relays}'
    assert n_sources==6, f'Tpch Q1 Fail: expected data from 6 sources found {n_sources}'

    global_order_count = df.count_order.sum()
    assert global_order_count==296539, f'Expected global order count of 296539, got {global_order_count}'

    global_quantity_count = (df.avg_qty * df.count_order).sum()
    global_weighted_avg_qty = global_quantity_count / global_order_count

    assert global_weighted_avg_qty==25.5228115020284, f'Expected global_weighted_avg_qty of 25.5228115020284, got {global_weighted_avg_qty}'