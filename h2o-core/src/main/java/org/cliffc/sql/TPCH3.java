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

Validation SF-0.01
47714|267010.5894|1995-03-11|0
22276|266351.5562|1995-01-29|0
32965|263768.3414|1995-02-25|0
21956|254541.1285|1995-02-02|0
1637|243512.7981|1995-02-08|0
10916|241320.0814|1995-03-11|0
30497|208566.6969|1995-02-07|0
450|205447.4232|1995-03-05|0
47204|204478.5213|1995-03-13|0
9696|201502.2188|1995-02-20|0

Validation SF-1
orderkey            orderdate      revenue  shippriority
 2456423  1995-03-04 16:00:00  406181.0111             0
 3459808  1995-03-03 16:00:00  405838.7000             0
  492164  1995-02-18 16:00:00   390324.061             0
 1188320  1995-03-08 16:00:00  384537.9359             0
 2435712  1995-02-25 16:00:00  378673.0558             0
 4878020  1995-03-11 16:00:00  378376.7952             0
 1163712  1995-03-14 16:00:00  377409.2753             0
 5521732  1995-03-12 16:00:00  375153.9215             0
 2628192  1995-02-21 16:00:00  373133.3094             0
  993600  1995-03-04 16:00:00  371407.4595             0

      SF0.01  SF1    SF10
Umbra         0.011
H2O   

SF-1  TPCH3 500 msec, 510 msec, 400 msec, 460 msec, 380 msec, 1430 msec, 380 msec, 380 msec, 450 msec, 380 msec, 
SF-10 TPCH3 4936 msec, 4825 msec, 4330 msec, 4480 msec, 4430 msec, 4310 msec, 4400 msec, 4350 msec, 3990 msec, 4310 msec, 

*/

public class TPCH3 implements SQL.TPCH {
  @Override public String name() { return "TPCH3"; }
  static final boolean PRINT_TIMING = true;
  
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
    Frame line2 = new SQL.FilterDate(line1.find("shipdate"),SHIPPED_DATE,Long.MAX_VALUE).doAll(line1.types(),line1).outputFrame(line1.names(),line1.domains());
    Frame line3 = line2.subframe(new String[]{"orderkey","extendedprice","discount"}); // Drop shipdate after filter

    // Filter ORDERS by date before, a 50% filter.
    Frame ords0 = SQL.ORDERS.frame(); // Filter by used columns
    assert ords0.vec("shippriority").isConst(); // TODO: optimize because constant column
    Frame ords1 = ords0.subframe(new String[]{"custkey","orderdate","orderkey"});
    Frame ords2 = new SQL.FilterDate(ords1.find("orderdate"),0,SHIPPED_DATE).doAll(ords1.types(),ords1).outputFrame(ords1.names(),ords1.domains());

    // Filter CUSTOMERS by SEGMENT, a 20% filter
    Frame custs0 = SQL.CUSTOMER.frame(); // Filter by used columns
    Frame custs1 = custs0.subframe(new String[]{"custkey","mktsegment"});
    int seg = ArrayUtils.find(custs1.vec("mktsegment").domain(),SEGMENT);
    Frame custs2 = new SQL.FilterCol(custs1.find("mktsegment"),seg).doAll(custs1.types(),custs1).outputFrame(custs1.names(),custs1.domains());
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
    cust_ords2 = SQL.compact(cust_ords2);
    Frame line_cuds = SQL.join(cust_ords2,line3); // 1032 rows
    cust_ords.delete();
    line2.delete();
    line_cuds = SQL.compact(line_cuds);
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

  private static class Revenue extends MRTask<Revenue> {
    @Override public void map( Chunk expr, Chunk disc, NewChunk rev ) {
      for( int i=0; i<expr._len; i++ )
        rev.addNum(expr.atd(i)*(1.0-disc.atd(i)));
    }
  }
  
}
