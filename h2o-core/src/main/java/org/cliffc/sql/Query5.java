package org.cliffc.sql;

import water.*;
import water.fvec.*;
import water.rapids.Merge;
import water.rapids.ast.prims.mungers.AstGroup;
import water.util.ArrayUtils;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

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
  static final boolean PRINT_TIMING = false;
  
  static final long LOW_DATE  = new DateTime("1994-01-01",DateTimeZone.UTC).getMillis();
  static final long HIGH_DATE = new DateTime("1994-01-01",DateTimeZone.UTC).plusYears(1).getMillis();
  static final String REGION = "ASIA";
  
  // Query plan:

  // guessing:
  // filter ORDERS by year-date.
  // Pre-join customers into NATION_REGION_SUPPLIER_PARTSUPP if it fits.
  // Filter N_R_S_P_CUST by region.
  // JOIN orders & linenumbers
  // GroupBy Name (small) & compute revenue sum

  
  @Override public Frame run() {
    long t = System.currentTimeMillis();
    // Filter orders by year
    Frame ords0 = SQL.ORDERS.frame(); // Filter by used columns
    Frame ords1 = ords0.subframe(new String[]{"orderkey","custkey","orderdate"});
    Frame ords2 = new SQL.FilterDate(ords1.find("orderdate"),LOW_DATE,HIGH_DATE).doAll(ords1.types(),ords1).outputFrame(ords1.names(),ords1.domains());
    Frame ords3 = ords2.subframe(new String[]{"orderkey","custkey"}); // Drop orderdate after filter

    // Filter NATION_REGION_SUPPLIER by region
    Frame nrs0 = SQL.NATION_REGION_SUPPLIER; // Filter by used columns
    Frame nrs1 = nrs0.subframe(new String[]{"suppkey","r_name","n_name"});
    int region = ArrayUtils.find(nrs1.vec("r_name").domain(),REGION);
    Frame nrs2 = new SQL.FilterCol(nrs1.find("r_name"),region).doAll(nrs1.types(),nrs1).outputFrame(nrs1.names(),nrs1.domains());
    Frame nrs3 = nrs2.subframe(new String[]{"suppkey","n_name"}); // Drop r_name after filter

    // Customers
    Frame cust0 = SQL.CUSTOMER.frame();
    Frame cust1 = cust0.subframe(new String[]{"custkey","n_name"});

    long t_filter = System.currentTimeMillis();
    if( PRINT_TIMING ) System.out.print("filter "+(t_filter-t)+" msec, "); t=t_filter;

    // Join NRS with customers on n_name
    Frame nrsc0 = SQL.join(nrs3,cust1);
    nrs3.delete();
    Frame nrsc1 = nrsc0.subframe(new String[]{"suppkey","n_name","custkey"}); // gain custkey
    long t_join0 = System.currentTimeMillis();
    if( PRINT_TIMING ) System.out.print("join0 "+(t_join0-t)+" msec, "); t=t_join0;

    // Join NRSC with orders using custkey
    Frame nrsco_0 = SQL.join(nrsc1,ords3);
    ords3.delete();
    nrsc0.delete();
    Frame nrsco_1 = nrsco_0.subframe(new String[]{"suppkey","n_name","orderkey"}); // drop custkey, gain orderkey
    long t_join1 = System.currentTimeMillis();
    if( PRINT_TIMING ) System.out.print("join1 "+(t_join1-t)+" msec, "); t=t_join1;

    // LineItems
    Frame line0 = SQL.LINEITEM.frame();
    Frame line1 = line0.subframe(new String[]{"orderkey","suppkey","extendedprice","discount"});

    // Join NRSCO with lineitem using orderkey,suppkey
    Frame nrscol_0 = SQL.join(nrsco_1,line1);
    nrsco_0.delete();
    long t_join2 = System.currentTimeMillis();
    if( PRINT_TIMING ) System.out.print("join2 "+(t_join2-t)+" msec, "); t=t_join2;

    // GroupBy n_name, computing revenue
    double[] n_revs = new GBRevenue().doAll(nrscol_0.vec("n_name"),nrscol_0.vec("extendedprice"),nrscol_0.vec("discount")).n_revs;
    long t_gb = System.currentTimeMillis();
    if( PRINT_TIMING ) System.out.print("GB "+(t_gb-t)+" msec, "); t=t_gb;

    // Format results
    String[] prs = nrscol_0.vec("n_name").domain();
    Vec vec = Vec.makeSeq(0,prs.length);
    vec.setDomain(prs);
    Frame rez0 = new Frame();
    rez0.add("n_name",vec);
    rez0.add("revenue",Vec.makeVec(n_revs,Vec.newKey()));

    Frame rez1 = Merge.sort(rez0,rez0.find(new String[]{"revenue"}),
                                           new int[]   {Merge.DESCENDING});
    rez0.delete();
    long t_format = System.currentTimeMillis();
    if( PRINT_TIMING ) System.out.print("Format "+(t_gb-t)+" msec, "); t=t_format;
    if( PRINT_TIMING ) System.out.println();
    
    return rez1;
  }

  
  // Filter by orderkeys and by date range; group-by orderpriority and compute counts.
  public static class GBRevenue extends MRTask<GBRevenue> {
    double[] n_revs;
    @Override public void map( Chunk n_names, Chunk exprices, Chunk discs ) {
      n_revs = new double[n_names.vec().cardinality()];
      // The Main Hot Loop
      for( int i=0; i<n_names._len; i++ )
        n_revs[(int)n_names.at8(i)] += exprices.atd(i)*(1.0-discs.atd(i));
    }
    @Override public void reduce( GBRevenue fkd ) {
      ArrayUtils.add(n_revs,fkd.n_revs);
    }
  }

}
