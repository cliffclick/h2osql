package org.cliffc.sql;

import water.*;
import water.nbhm.NonBlockingSetInt;
import water.fvec.*;
import water.rapids.Merge;
import water.rapids.ast.prims.mungers.AstGroup;
import water.util.ArrayUtils;
import java.util.Arrays;
import org.joda.time.DateTime;

/**
The Order Priority Checking Query counts the number of orders ordered in a given
quarter of a given year in which at least one lineitem was received by the
customer later than its committed date. The query lists the count of such orders
for each order priority sorted in ascending priority order.

select
    o_orderpriority,
    count( * ) as order_count
from
    orders
where
    o_orderdate >= date '@@1'
    and o_orderdate < date '@@1' + interval '3' month
    and exists (
        select
            *
        from
            lineitem
        where
            l_orderkey = o_orderkey
            and l_commitdate < l_receiptdate
    )
group by
    o_orderpriority
order by
    o_orderpriority;

*/

public class Query4 implements SQL.Query {
  @Override public String name() { return "Query4"; }
  
  static final long LOW_DATE  = new DateTime("1993-07-01").getMillis();
  static final long HIGH_DATE = new DateTime("1993-07-01").plusMonths(3).getMillis();

  @Override public Frame run() {
    long t = System.currentTimeMillis();

    // Filter LINEITEMS by commit < receipt, a 50% filter, and keep the matching orderkeys
    Frame line0 = SQL.LINEITEM.frame(); // Filter by used columns
    Frame line1 = line0.subframe(new String[]{"orderkey","commitdate","receiptdate"});
    NonBlockingSetInt ordkeys = new FilterLate().doAll(line1)._nbsi;
    System.out.println(ordkeys.length());

    // Filter ORDERS by date range and late ordkeys
    Frame ords0 = SQL.ORDERS.frame(); // Filter by used columns
    Frame ords1 = ords0.subframe(new String[]{"orderkey","orderdate","orderpriority"});
    int[] pr_cnts = new FilterKeysDate(LOW_DATE,HIGH_DATE,ordkeys).doAll(ords1).pr_cnts;
    System.out.println(Arrays.toString(pr_cnts));

    // 

    return null;
  }

  // Filter by date, then save matching orderkeys in a bitset
  public static class FilterLate extends MRTask<FilterLate> {
    final NonBlockingSetInt _nbsi = new NonBlockingSetInt();
    @Override public void map( Chunk orderkeys, Chunk commits, Chunk receipts ) {
      // The Main Hot Loop
      for( int i=0; i<commits._len; i++ )
        if( commits.at8(i) < receipts.at8(i) )
          _nbsi.add((int)orderkeys.at8(i));
    }
    @Override public void reduce( FilterLate fl ) {
      if( _nbsi != fl._nbsi )
        throw new RuntimeException("distributed reduce not implemented");
    }
  }

  // Filter by orderkeys and by date range; group-by orderpriority and compute counts.
  public static class FilterKeysDate extends MRTask<FilterKeysDate> {
    final long _lo, _hi;
    final NonBlockingSetInt _ordkeys;
    int[] pr_cnts;
    FilterKeysDate(long lo_date, long hi_date, NonBlockingSetInt ordkeys) { _lo = lo_date; _hi = hi_date; _ordkeys = ordkeys; }
    @Override public void map( Chunk ordkeys, Chunk dates, Chunk prioritys ) {
      pr_cnts = new int[prioritys.vec().cardinality()];
      
      // The Main Hot Loop
      for( int i=0; i<dates._len; i++ ) {
        long date = dates.at8(i);
        int ordkey = (int)ordkeys.at8(i);
        if( _ordkeys.contains(ordkey) && _lo <= date && date < _hi )
          pr_cnts[(int)prioritys.at8(i)]++;
      }
    }
    @Override public void reduce( FilterKeysDate fkd ) {
      ArrayUtils.add(pr_cnts,fkd.pr_cnts);
    }
  }
  
}
