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

public class TPCH5 implements SQL.TPCH {
  @Override public String name() { return "TPCH5"; }
  static final boolean PRINT_TIMING = false;
  
  static final long LOW_DATE  = new DateTime("1994-01-01",DateTimeZone.UTC).getMillis();
  static final long HIGH_DATE = new DateTime("1994-01-01",DateTimeZone.UTC).plusYears(1).getMillis();
  static final String REGION = "ASIA";
  
  // Query plan:

  // Filter ORDERS by year-date.
  // Filter NATION_REGION_SUPPLIER by REGION
  // Join NRS with CUSTOMER by n_name
  // Join NRSC with ORDERS by custkey (most expensive step)
  // Compute Small data unique orderkeys, and nation-per-suppkey-per-orderkey.
  //   Works because unique orderkeys is about 1/400 NRSC; should work up to SF100.
  //   Avoids a final Join of NRSCO with LINEITEM - which is far more expensive.
  // Filter LINEITEM by matching orderkey+suppkey; GroupBy nation & compute revenue.
  
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
    Frame nrsco_1 = nrsco_0.subframe(new String[]{"orderkey","suppkey","n_name"}); // drop custkey, gain orderkey
    long t_join1 = System.currentTimeMillis();
    if( PRINT_TIMING ) System.out.print("join1 "+(t_join1-t)+" msec, "); t=t_join1;

    // LineItems
    Frame line0 = SQL.LINEITEM.frame();
    Frame line1 = line0.subframe(new String[]{"orderkey","suppkey","extendedprice","discount"});
    
    // Gather suppkeys-per-unique-ord, and n_name-per-suppkey
    NonBlockingHashMapLong<NonBlockingHashMapLong<Integer>> uniqords = new UniqueOrders().doAll(nrsco_1.vecs())._uniqords;
    long t_uniq = System.currentTimeMillis();
    if( PRINT_TIMING ) System.out.print("uniques "+(t_uniq-t)+" msec, "); t=t_uniq;

    // Filter out non-matching order/supp pairs.
    // GroupBy nation and compute revenue.
    double[] n_revs = new FilterLNGB(nrs0.vec("n_name").cardinality(),uniqords).doAll(line1).n_revs;    
    long t_gb = System.currentTimeMillis();
    if( PRINT_TIMING ) System.out.print("GB "+(t_gb-t)+" msec, "); t=t_gb;
    
    // Format results
    String[] prs = nrs0.vec("n_name").domain();
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

  private static class UniqueOrders extends MRTask<UniqueOrders> {
    transient NonBlockingHashMapLong<NonBlockingHashMapLong<Integer>> _uniqords;
    @Override protected void setupLocal() { _uniqords = new NonBlockingHashMapLong<NonBlockingHashMapLong<Integer>>(10000); }
    @Override public void map(Chunk ordkeys, Chunk suppkeys, Chunk nnames) {
      for( int i=0; i<ordkeys._len; i++ ) {
        long ordkey =     ordkeys .at8(i);
        int suppkey= (int)suppkeys.at8(i);
        Integer nname = Integer.valueOf((int)nnames.at8(i));
        NonBlockingHashMapLong<Integer> nbsi = _uniqords.get(ordkey);
        if( nbsi==null ) {
          NonBlockingHashMapLong<Integer> nbsi2 = new NonBlockingHashMapLong<>();
          nbsi = _uniqords.putIfAbsent(ordkey, nbsi2);
          if( nbsi==null ) nbsi = nbsi2; // Use the old value, or the new value if no old value
        }
        nbsi.put(suppkey,nname);
      }
    }
    @Override public void reduce( UniqueOrders uqo ) {
      if( _uniqords != uqo._uniqords )
        throw new RuntimeException("distributed reduce not implemented");
    }
  }

  private static class FilterLNGB extends MRTask<FilterLNGB> {
    final NonBlockingHashMapLong<NonBlockingHashMapLong<Integer>> _uniqords;
    final int _nation_card;
    double[] n_revs;
    FilterLNGB( int nation_card, NonBlockingHashMapLong<NonBlockingHashMapLong<Integer>> uniqords ) {
      _uniqords = uniqords;
      _nation_card = nation_card;
    }
    @Override public void map( Chunk[] cs ) {
      Chunk l_ordks = cs[0], l_supks = cs[1], exprices = cs[2], discs = cs[3];
      n_revs = new double[_nation_card]; // revenue-by-nation
      for( int i=0; i<l_ordks._len; i++ ) {
        long ordkey = l_ordks.at8(i);
        NonBlockingHashMapLong<Integer> nbsi = _uniqords.get(ordkey);
        if( nbsi==null ) continue; // No matching order
        Integer in = nbsi.get(l_supks.at8(i));
        if( in!=null )          // No matching suppkey
          n_revs[in] += exprices.atd(i)*(1.0-discs.atd(i));
      }
    }
    @Override public void reduce( FilterLNGB flngb ) {
      ArrayUtils.add(n_revs,flngb.n_revs);
    }
  }

  // Filter by orderkeys and by date range; group-by orderpriority and compute counts.
  static class GBRevenue extends MRTask<GBRevenue> {
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
