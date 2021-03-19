package org.cliffc.sql;

import water.*;
import water.fvec.*;
//import water.rapids.Merge;
//import water.rapids.ast.prims.mungers.AstGroup;
//import water.nbhm.NonBlockingHashMapLong;
//import water.util.ArrayUtils;
//import water.util.VecUtils;
//import java.util.Arrays;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

/**
The Forecasting Revenue Change Query considers all the lineitems shipped in a
given year with discounts between DISCOUNT-0.01 and DISCOUNT+0.01. The query
lists the amount by which the total revenue would have increased if these
discounts had been eliminated for lineitems with l_quantity less than
quantity. Note that the potential revenue increase is equal to the sum of
[l_extendedprice * l_discount] for all lineitems with discounts and quantities
in the qualifying range.

select
    sum(l_extendedprice * l_discount) as revenue
from
    lineitem
where
    l_shipdate >= date '@@1'
    and l_shipdate < date '@@1' + interval '1' year
    and l_discount between @@2 - 0.01 and @@2 + 0.01
    and l_quantity < @@3;

*/

public class Query7 implements SQL.Query {
  @Override public String name() { return "Query7"; }
  static final boolean PRINT_TIMING = false;

  static final long LOW_DATE  = new DateTime("1994-01-01",DateTimeZone.UTC).getMillis();
  static final long HIGH_DATE = new DateTime("1994-01-01",DateTimeZone.UTC).plusYears(1).getMillis();
  static final double DISCOUNT = 0.06;
  static final double EPSILON = 1e-10;
  static final long QUANTITY = 24;
  
  @Override public Frame run() {
    return null;
  }
  
}
