package org.cliffc.sql;

import water.*;
import water.fvec.*;
import water.rapids.Merge;
import water.rapids.ast.prims.mungers.AstGroup;
import water.util.ArrayUtils;
import org.joda.time.DateTime;

/**
The Local Supplier Volume Query lists for each nation in a region the revenue
volume that resulted from lineitem transactions in which the customer ordering
parts and the supplier filling them were both within that nation. The query is
run in order to determine whether to institute local distribution centers in a
given region. The query considers only parts ordered in a given year. The query
displays the nations and revenue volume in descending order by revenue. Revenue
volume for all qualifying lineitems in a particular nation is defined as
sum(l_extendedprice * (1 - l_discount)).

REGION is randomly selected within the list of values defined for R_NAME.
DATE is the first of January of a randomly selected year within [1993 .. 1997].

select
    n_name,
    sum(l_extendedprice * (1 - l_discount)) as revenue
from
    customer,
    orders,
    lineitem,
    supplier,
    nation,
    region
where
    c_custkey = o_custkey
    and l_orderkey = o_orderkey
    and l_suppkey = s_suppkey
    and c_nationkey = s_nationkey
    and s_nationkey = n_nationkey
    and n_regionkey = r_regionkey
    and r_name = '@@1'
    and o_orderdate >= date '@@2'
    and o_orderdate < date '@@2' + interval '1' year
group by
    n_name
order by
    revenue desc;

*/

public class Query5 implements SQL.Query {
  @Override public String name() { return "Query5"; }
  
  // Query plan:

  // guessing:
  // filter ORDERS by year-date.
  // Pre-join customers into NATION_REGION_SUPPLIER_PARTSUPP if it fits.
  // Filter N_R_S_P_CUST by region.
  // JOIN orders & linenumbers
  // GroupBy Name (small) & compute revenue sum

  
  @Override public Frame run() {
    return null;
  }
  
}
