package org.cliffc.sql;

import water.*;
import water.fvec.*;
import water.util.ArrayUtils;
import org.joda.time.DateTime;

/**
The Shipping Priority Query retrieves the shipping priority and potential
revenue, defined as the sum of l_extendedprice * (1-l_discount), of the orders
having the largest revenue among those that had not been shipped as of a given
date. Orders are listed in decreasing order of revenue. If more than 10
unshipped orders exist, only the 10 orders with the largest revenue are listed

Return the first 10 selected rows
select
    l_orderkey,
    sum(l_extendedprice * (1 - l_discount)) as revenue,
    o_orderdate,
    o_shippriority
from
    customer,
    orders,
    lineitem
where
    c_mktsegment = '@@1'
    and c_custkey = o_custkey
    and l_orderkey = o_orderkey
    and o_orderdate < date '@@2'
    and l_shipdate > date '@@2'
group by
    l_orderkey,
    o_orderdate,
    o_shippriority
order by
    revenue desc,
    o_orderdate;

*/

public class Query3 implements SQL.Query {
  @Override public String name() { return "Query3"; }
  
  static final String SEGMENT = "BUILDING";
  static final long SHIPPED_DATE = new DateTime("1995-03-15").getMillis();

  // Query plan:

  // Filter orders and lineitems by shipdate; a 50% filter.
  // Filter customers by segment.
  // JOIN them all.
  // Run a Groupby, computing revenue
  // Sort.
  
  @Override public Frame run() {
    // Filter LINEITEM by date after, a 50% filter.
    Frame line0 = SQL.LINEITEM.frame(); // Filter by used columns
    Frame line1 = line0.subframe(new String[]{"orderkey","shipdate","extendedprice","discount"});
    Frame line2 = new FilterDate(line1.find("shipdate"),SHIPPED_DATE,false).doAll(line1.types(),line1).outputFrame(line1.names(),line1.domains());

    // Filter ORDERS by date before, a 50% filter.
    Frame ords0 = SQL.ORDERS.frame(); // Filter by used columns
    Frame ords1 = ords0.subframe(new String[]{"custkey","orderdate","shippriority","orderkey"});
    Frame ords2 = new FilterDate(ords1.find("orderdate"),SHIPPED_DATE,true).doAll(ords1.types(),ords1).outputFrame(ords1.names(),ords1.domains());

    // Filter CUSTOMERS by SEGMENT, a 20% filter
    Frame custs0 = SQL.CUSTOMER.frame(); // Filter by used columns
    Frame custs1 = custs0.subframe(new String[]{"custkey","mktsegment"});
    int seg = ArrayUtils.find(custs1.vec("mktsegment").domain(),SEGMENT);
    Frame custs2 = new FilterCol(custs1.find("mktsegment"),seg).doAll(custs1.types(),custs1).outputFrame(custs1.names(),custs1.domains());

    // JOIN (customers and orders) and lineitems
    Frame cust_ords = SQL.join(ords2,custs2);
    ords2.delete();
    custs2.delete();
    Frame line_cuds = SQL.join(cust_ords,line2);
    cust_ords.delete();
    line2.delete();

    // Run a GroupBy(orderkey,orderdate,shippriority) and compute revenue
    System.out.println(line_cuds.toTwoDimTable(0,10,true));
    //System.out.println(SQL.histo(line_cuds,"orderkey"));
    //System.out.println(SQL.histo(line_cuds,"orderdate"));
    //System.out.println(SQL.histo(line_cuds,"shippriority"));
    System.out.println(line_cuds.vec("shippriority").isConst());

    

    return line_cuds;
  }

  // Filter by TYPE and SIZE.  Reduces dataset by ~50x
  private static class FilterDate extends MRTask<FilterDate> {
    final boolean _before;
    final int _datex;
    final long _date;
    FilterDate(int datex, long date, boolean before) { _datex=datex; _date=date; _before=before; }
    @Override public void map( Chunk[] cs, NewChunk[] ncs ) {
      Chunk dates = cs[_datex];
      // The Main Hot Loop
      if( _before ) for( int i=0; i<dates._len; i++ ) { if( dates.at8(i) < _date ) SQL.copyRow(cs,ncs,i); }
      else          for( int i=0; i<dates._len; i++ ) { if( dates.at8(i) > _date ) SQL.copyRow(cs,ncs,i); }
    }
  }

  // Filter int column by exact match
  private static class FilterCol extends MRTask<FilterCol> {
    final int _colx, _e;
    FilterCol(int colx, int e) { _colx = colx; _e = e; }
    @Override public void map( Chunk[] cs, NewChunk[] ncs ) {
      Chunk datas = cs[_colx];
      // The Main Hot Loop
      for( int i=0; i<datas._len; i++ )
        if( datas.at8(i) == _e )
          SQL.copyRow(cs,ncs,i);
    }
  }


  
}
