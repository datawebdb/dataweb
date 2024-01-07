def make_query1():
    return  {'sql':
    'select {linenumber}, {tax} from {lineitem} where {tax_pct}>3 limit 10',
  'substitution_blocks':{
    'info_substitutions':{
      'linenumber': {'entity_name': 'lineitem', 'info_name': 'linenumber', 'include_info': True, 'exclude_info_alias': False, 'include_data_field':False},
      'tax': {'entity_name': 'lineitem', 'info_name': 'tax_amount', 'include_info': True, 'exclude_info_alias': False, 'include_data_field':False},
      'tax_pct': {'entity_name': 'lineitem', 'info_name': 'tax_percent', 'include_info': True, 'exclude_info_alias': True, 'include_data_field':False}
      },
    'source_substitutions': {
      'lineitem': {'AllSourcesWith': ['lineitem']}
  }
  }
  }

def make_query2():
    return {'sql':
    'select * from {lineitem} where {tax_val}>3 limit 10000',
    'substitution_blocks':{
    'info_substitutions':{
        'tax_val': {'entity_name': 'lineitem', 'info_name': 'tax_percent', 'include_info': True, 'exclude_info_alias': True, 'include_data_field':False}
        },
    'source_substitutions': {
        'lineitem': {'AllSourcesWith': ['lineitem']}
    }
    }
    }

def make_query3():
    return {'sql':
    'with a as (select * from {user_tables}) select * from user_data_folder',
    'substitution_blocks':{
    'info_substitutions':{
        'tax_val': {'entity_name': 'lineitem', 'info_name': 'tax_percent', 'include_info': False, 'exclude_info_alias': False, 'include_data_field':True}
        },
    'source_substitutions': {
        'lineitem': {'AllSourcesWith': ['lineitem']}
    }
    }
    }

def make_query4():
    # In the case we need to include {} in literal filters, we can increase the num_capture_braces so the template is unambiguous
    return {'sql':
    "select {{linenumber}}, {{tax}} from {{lineitem}} where {{tax_val}}>3 and '{tax_val}'='{tax_val}'  limit 10",
  'substitution_blocks':{
    'info_substitutions':{
      'linenumber': {'entity_name': 'lineitem', 'info_name': 'linenumber', 'include_info': True, 'exclude_info_alias': False, 'include_data_field':False},
      'tax': {'entity_name': 'lineitem', 'info_name': 'tax_percent', 'include_info': True, 'exclude_info_alias': False, 'include_data_field':False},
      'tax_val': {'entity_name': 'lineitem', 'info_name': 'tax_percent', 'include_info': True, 'exclude_info_alias': True, 'include_data_field':False}
      },
    'source_substitutions': {
      'lineitem': {'AllSourcesWith': ['lineitem']}
  },
    'num_capture_braces': 2,
  }
  }

def make_tpch_q1_query():
    tpch1 = {}
    tpch1['sql'] = """select
    {returnflag},
    {linestatus},
    sum({quantity}) as sum_qty,
    sum({extendedprice}) as sum_base_price,
    sum({extendedprice} * (1 - {discount_percent}/100)) as sum_disc_price,
    sum({extendedprice} * (1 - {discount_percent}/100) * (1 + {tax_percent}/100)) as sum_charge,
    avg({quantity}) as avg_qty,
    avg({extendedprice}) as avg_price,
    avg({discount_percent}/100) as avg_disc,
    count(*) as count_order
from
    {lineitem}
where
        {shipdate} <= date '1998-09-02'
group by
    {returnflag},
    {linestatus}
order by
    {returnflag},
    {linestatus}"""

    subtitution_blocks = {'info_substitutions': {}, 'source_substitutions': {}}
    info_subs = ['returnflag', 'linestatus', 'quantity', 'extendedprice', 
                 'discount_percent', 'tax_percent','shipdate']
    
    for info in info_subs:
        subtitution_blocks['info_substitutions'][info] = \
            {'entity_name': 'lineitem', 'info_name': info, 
            'include_info': True, 'exclude_info_alias': True, 'include_data_field': False}

    subtitution_blocks['source_substitutions']['lineitem'] = {'AllSourcesWith': ['lineitem']}

    tpch1['substitution_blocks'] = subtitution_blocks
    return tpch1
