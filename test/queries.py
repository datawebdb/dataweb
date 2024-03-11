def make_query1():
    return  {'sql':
    'select linenumber, tax_amount from lineitem where tax_percent>3 limit 10',
  }

def make_query2():
    return {'sql':
    'select * from lineitem where tax_percent>3 limit 10000',
    }

def make_query3():
    return {'sql':
    'with a as (select * from user_tables) select * from user_data_folder',
    }

def make_query4():
    # In the case we need to include {} in literal filters, we can increase the num_capture_braces so the template is unambiguous
    return {'sql':
    "select linenumber, tax_percent from lineitem where tax_percent>3 and '{tax_val}'='{tax_val}'  limit 10",
  }

def make_tpch_q1_query():
    tpch1 = {'sql':
    """select
    returnflag,
    linestatus,
    sum(quantity) as sum_qty,
    sum(extendedprice) as sum_base_price,
    sum(extendedprice * (1 - discount_percent/100)) as sum_disc_price,
    sum(extendedprice * (1 - discount_percent/100) * (1 + tax_percent/100)) as sum_charge,
    avg(quantity) as avg_qty,
    avg(extendedprice) as avg_price,
    avg(discount_percent/100) as avg_disc,
    count(*) as count_order
from
    lineitem
where
        shipdate <= date '1998-09-02'
group by
    returnflag,
    linestatus
order by
    returnflag,
    linestatus"""}

    return tpch1
