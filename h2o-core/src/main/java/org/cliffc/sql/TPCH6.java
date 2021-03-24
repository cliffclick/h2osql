package org.cliffc.sql;

import water.*;
import water.fvec.*;
import water.rapids.Merge;
import water.rapids.ast.prims.mungers.AstGroup;
import water.nbhm.NonBlockingHashMapLong;
import water.util.ArrayUtils;
import water.util.VecUtils;
import java.util.Arrays;
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

public class TPCH6 implements SQL.TPCH {
  @Override public String name() { return "TPCH6"; }
  static final boolean PRINT_TIMING = false;

  static final long LOW_DATE  = new DateTime("1994-01-01",DateTimeZone.UTC).getMillis();
  static final long HIGH_DATE = new DateTime("1994-01-01",DateTimeZone.UTC).plusYears(1).getMillis();
  static final double DISCOUNT = 0.06;
  static final double EPSILON = 1e-10;
  static final long QUANTITY = 24;
  
  @Override public Frame run() {
    Frame line0 = SQL.LINEITEM.frame();
    Frame line1 = line0.subframe(new String[]{"shipdate","discount","quantity","extendedprice"});
    double sum = new FilterSum(line1).doAll(line1)._sum;
    // Format results
    Frame fr = new Frame();
    fr.add("revenue",Vec.makeVec(new double[]{sum},Vec.newKey()));
    return fr;
  }

  private static class FilterSum extends MRTask<FilterSum> {
    final int _spx, _dsx, _qtx, _exx;
    double _sum;
    FilterSum( Frame lineitem ) {
      _spx = lineitem.find("shipdate");
      _dsx = lineitem.find("discount");
      _qtx = lineitem.find("quantity");
      _exx = lineitem.find("extendedprice");
    }
    @Override public void map( Chunk[] cs ) {
      Chunk shipdate = cs[_spx];
      Chunk discount = cs[_dsx];
      Chunk quantity = cs[_qtx];
      Chunk extended = cs[_exx];
      double sum=0;
      for( int i=0; i<shipdate._len; i++ ) {
        long date = shipdate.at8(i);
        if( LOW_DATE <= date && date < HIGH_DATE ) {
          long quant = quantity.at8(i);
          if( quant < QUANTITY ) {
            double disc = discount.atd(i);
            if( DISCOUNT-0.01-EPSILON <= disc && disc <= DISCOUNT+0.01+EPSILON )
              sum += extended.atd(i)*disc;
          }
        }
      }
      _sum = sum;
    }
    @Override public void reduce( FilterSum fs ) { _sum += fs._sum; }
  }
  
}
