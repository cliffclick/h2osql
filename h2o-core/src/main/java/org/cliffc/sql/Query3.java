package org.cliffc.sql;

import water.*;
import water.fvec.*;
import water.rapids.Merge;
import water.rapids.ast.prims.mungers.AstGroup;
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
  // Filter customers by segment; a 20% filter; all filters take 15% of query time.
  // JOIN them all; takes 80% of query time.
  // Run a Groupby, computing revenue
  // Sort.
  
  @Override public Frame run() {
    long t = System.currentTimeMillis();
    // Run 3 filters over Big Data.  Takes ~15% of query time.
    // Filter LINEITEM by date after, a 50% filter.
    Frame line0 = SQL.LINEITEM.frame(); // Filter by used columns
    Frame line1 = line0.subframe(new String[]{"orderkey","shipdate","extendedprice","discount"});
    Frame line2 = new FilterDate(line1.find("shipdate"),SHIPPED_DATE,false).doAll(line1.types(),line1).outputFrame(line1.names(),line1.domains());
    Frame line3 = line2.subframe(new String[]{"orderkey","extendedprice","discount"}); // Drop shipdate after filter

    // Filter ORDERS by date before, a 50% filter.
    Frame ords0 = SQL.ORDERS.frame(); // Filter by used columns
    assert ords0.vec("shippriority").isConst(); // TODO: optimize because constant column
    Frame ords1 = ords0.subframe(new String[]{"custkey","orderdate","orderkey"});
    Frame ords2 = new FilterDate(ords1.find("orderdate"),SHIPPED_DATE,true).doAll(ords1.types(),ords1).outputFrame(ords1.names(),ords1.domains());

    // Filter CUSTOMERS by SEGMENT, a 20% filter
    Frame custs0 = SQL.CUSTOMER.frame(); // Filter by used columns
    Frame custs1 = custs0.subframe(new String[]{"custkey","mktsegment"});
    int seg = ArrayUtils.find(custs1.vec("mktsegment").domain(),SEGMENT);
    Frame custs2 = new FilterCol(custs1.find("mktsegment"),seg).doAll(custs1.types(),custs1).outputFrame(custs1.names(),custs1.domains());
    Frame custs3 = custs2.subframe(new String[]{"custkey"});
    long t_filter = System.currentTimeMillis();
    //System.out.print("filter "+(t_filter-t)+" msec, "); t=t_filter;


    // TODO: You could imagine filtering customers, then making a BitSet with
    // custkey, passing the BitSet to filter ORDERS by cust & date.  This
    // smaller set of orders then JOINs with lineitems.
    
    // JOIN (customers and orders) and lineitems.  Takes ~80% of query time.
    // Reduces data by ~40x (SF 0.01, 39K rows -> 1K rows)
    Frame cust_ords = SQL.join(ords2,custs3);
    ords2.delete();
    custs2.delete();
    Frame cust_ords2 = cust_ords.subframe(new String[]{"orderdate","orderkey"}); // Drop custkey after join
    Frame line_cuds = SQL.join(cust_ords2,line3); // 1032 rows
    cust_ords.delete();
    line2.delete();
    long t_joins = System.currentTimeMillis();
    //System.out.print("joins "+(t_joins-t)+" msec, "); t=t_joins;
    
    // Add a revenue column
    Vec rev = new Revenue().doAll(Vec.T_NUM,line_cuds.vecs(new String[]{"extendedprice","discount"})).outputFrame().anyVec();
    line_cuds.add("revenue",rev); 
    long t_rev = System.currentTimeMillis();
    //System.out.print("rev "+(t_rev-t)+" msec, "); t=t_rev;
    
    // Run a GroupBy(orderkey,orderdate/*,shippriority*/) and compute sum_revenue
    int[] gbCols = line_cuds.find(new String[]{"orderkey","orderdate"});
    AstGroup.AGG agg = new AstGroup.AGG(AstGroup.FCN.sum,line_cuds.find("revenue"),AstGroup.NAHandling.RM,0);
    Frame rez0 = new AstGroup().performGroupingWithAggregations(line_cuds,gbCols,new AstGroup.AGG[]{agg}).getFrame();
    rez0.names()[2] = "revenue";  // Rename sum_revenue back to revenue
    long t_gb = System.currentTimeMillis();
    //System.out.print("groupby "+(t_gb-t)+" msec, "); t=t_gb;

    // Sort by revenue and orderdate
    Frame rez1 = Merge.sort(rez0,rez0.find(new String[]{"revenue",       "orderdate"    }),
                                           new int[]   {Merge.DESCENDING,Merge.ASCENDING});
    rez0.delete();
    long t_sort = System.currentTimeMillis();
    //System.out.print("sort "+(t_sort-t)+" msec, "); t=t_sort;
    //System.out.println();

    // Add in the constant shippriority column
    rez1.add("shippriority",rez1.anyVec().makeZero());
    
    return rez1;
  }

  // Filter by before/after date.
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

  private static class Revenue extends MRTask<Revenue> {
    @Override public void map( Chunk expr, Chunk disc, NewChunk rev ) {
      for( int i=0; i<expr._len; i++ )
        rev.addNum(expr.atd(i)*(1.0-disc.atd(i)));
    }
  }
  
}
