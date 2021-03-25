package org.cliffc.sql;

import water.*;
import water.fvec.*;
import water.rapids.Merge;
import water.rapids.ast.prims.mungers.AstGroup;
import water.nbhm.NonBlockingHashMapLong;
import water.util.ArrayUtils;
import water.util.VecUtils;

import java.util.Arrays;
import java.util.BitSet;

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

VIETNAM   |1000926.6999
CHINA     |740210.757
JAPAN     |660651.2425
INDONESIA |566379.5276
INDIA     |422874.6844

*/

public class TPCH5 implements SQL.TPCH {
  @Override public String name() { return "TPCH5"; }
  static final boolean PRINT_TIMING = false;

  static final long LOW_DATE  = new DateTime("1994-01-01",DateTimeZone.UTC).getMillis();
  static final long HIGH_DATE = new DateTime("1994-01-01",DateTimeZone.UTC).plusYears(1).getMillis();
  static final String REGION = "ASIA";

  // Umbra query plan:
  // - hash/join NATION + REGION on regionkey; filter REGION by ASIA; card==5
  // - hash/join NATION_REGION + CUSTOMER on nationkey; card==30k
  // - hash/join NATION_REGION_CUSTOMER + ORDERS on custkey; filter ORDERS by date; card==52k
  // - hash/join NATION_REGION_CUSTOMER_ORDERS + LINEITEM on orderkey; card==193k
  // - hash/join NRCOL + SUPPLIERS; card==193k
  // - groupby; card==5

  @Override public Frame run() {
    long t0 = System.currentTimeMillis();

    // - hash/join NATION+REGION on regionkey; filter REGION by ASIA; card==5
    // Results in a set of nations.
    int regionkey = ArrayUtils.find(SQL.REGION.frame().vec("r_name").domain(),REGION);
    Frame nation = SQL.NATION.frame();
    Vec.Reader nation_r_name = nation.vec("r_name").new Reader();
    BitSet nationkeys = new BitSet();
    for( int i=0; i<nation_r_name.length(); i++ )
      if( nation_r_name.at8(i) == regionkey )
        nationkeys.set(i);
    if( PRINT_TIMING ) { long t=System.currentTimeMillis(); System.out.println("NR#"+(nationkeys.cardinality())+", "+(t-t0)+" msec"); t0=t; }

    // - hash/join NATION_REGION + CUSTOMER on nationkey; card==30k
    // Result is a map of custkeys->nationkeys
    Frame customer = SQL.CUSTOMER.frame();
    NonBlockingHashMapLong<Integer> custkeys = new NRC(nationkeys).doAll(customer.vec("n_name"),customer.vec("custkey"))._custkeys;
    if( PRINT_TIMING ) { long t=System.currentTimeMillis(); System.out.println("NRC#"+(custkeys.size())+", "+(t-t0)+" msec"); t0=t; }

    // - hash/join NATION_REGION_CUSTOMER + ORDERS on custkey; filter ORDERS by date; card==52k
    // Result is a map of orderkeys->nationkeys
    Frame orders = SQL.ORDERS.frame();
    NonBlockingHashMapLong orderkeys = new NRCO(custkeys).doAll(orders.vec("custkey"),orders.vec("orderkey"),orders.vec("orderdate"))._orderkeys;
    if( PRINT_TIMING ) { long t=System.currentTimeMillis(); System.out.println("NRCO#"+(orderkeys.size())+", "+(t-t0)+" msec"); t0=t; }

    // - hash/join NATION_REGION_CUSTOMER_ORDERS + LINEITEM on orderkey; card==193k
    // - Filter orderkey->nationkey == supplier[l_suppkey].nationkey
    // Result is a groupby[nationkey] += l.extendprice*(1.-l.discount)
    Vec nation_n_name = nation.vec("n_name");
    int nationcard = (int)nation_n_name.cardinality();
    Vec s_nationkey = SQL.SUPPLIER.frame().vec("n_name");
    Frame lineitem = SQL.LINEITEM.frame();
    Frame lineitem2 = lineitem.subframe(new String[]{"orderkey","suppkey","extendedprice","discount"});
    double[] revenues = new NRCOL(nationcard,s_nationkey,orderkeys).doAll(lineitem2)._revenues;
    if( PRINT_TIMING ) { long t=System.currentTimeMillis(); System.out.println("NRCOL#"+(revenues.length)+", "+(t-t0)+" msec"); t0=t; }

    // Format results
    String[] prs = nation_n_name.domain();
    Vec vec = Vec.makeSeq(0,prs.length);
    vec.setDomain(prs);
    Frame rez0 = new Frame();
    rez0.add("n_name",vec);
    rez0.add("revenue",Vec.makeVec(revenues,Vec.newKey()));

    Frame rez1 = Merge.sort(rez0,rez0.find(new String[]{"revenue"}),
                                           new int[]   {Merge.DESCENDING});
    rez0.delete();
    long t_format = System.currentTimeMillis();
    if( PRINT_TIMING ) { long t=System.currentTimeMillis(); System.out.println("Formatting "+(t-t0)+" msec"); t0=t; }
    
    return rez1;
  }


  private static class NRC extends MRTask<NRC> {
    transient NonBlockingHashMapLong<Integer> _custkeys;
    final BitSet _nationkeys;
    NRC( BitSet nationkeys ) { _nationkeys = nationkeys; }
    @Override protected void setupLocal() { _custkeys = new NonBlockingHashMapLong<>(); }
    @Override public void map( Chunk n_names, Chunk custkeys ) {
      for( int i=0; i<n_names._len; i++ ) {
        int nationkey = (int)n_names.at8(i);
        if( _nationkeys.get(nationkey) )
          _custkeys.put((long)custkeys.at8(i),(Integer)nationkey);
      }
    }
    @Override public void reduce( NRC bld ) {
      if( _custkeys != bld._custkeys )
        throw new RuntimeException("distributed reduce not implemented");
    }
  }

  private static class NRCO extends MRTask<NRCO> {
    transient NonBlockingHashMapLong<Integer> _orderkeys;
    final NonBlockingHashMapLong<Integer> _custkeys;
    NRCO( NonBlockingHashMapLong<Integer> custkeys ) { _custkeys = custkeys; }
    @Override protected void setupLocal() { _orderkeys = new NonBlockingHashMapLong<>(_custkeys.size()*4); }
    @Override public void map( Chunk custkeys, Chunk orderkeys, Chunk orderdates ) {
      for( int i=0; i<custkeys._len; i++ ) {
        long date = orderdates.at8(i);
        if( LOW_DATE <= date && date < HIGH_DATE ) {
          Integer nationkey = _custkeys.get(custkeys.at8(i));
          if( nationkey != null )
            _orderkeys.put(orderkeys.at8(i),nationkey);
        }
      }
    }
    @Override public void reduce( NRCO bld ) {
      if( _orderkeys != bld._orderkeys )
        throw new RuntimeException("distributed reduce not implemented");
    }
  }

  private static class NRCOL extends MRTask<NRCOL> {
    double[] _revenues;
    final int _nationcard;
    final Vec _s_nationkey;
    final NonBlockingHashMapLong<Integer> _orderkeys;
    NRCOL( int nationcard, Vec s_nationkey, NonBlockingHashMapLong<Integer> orderkeys ) {
      _nationcard=nationcard;
      _s_nationkey = s_nationkey;
      _orderkeys = orderkeys;
    }
    @Override public void map( Chunk[] cs ) {
      Chunk orderkeys = cs[0];
      Chunk suppkeys  = cs[1];
      Chunk extendprs = cs[2];
      Chunk discounts = cs[3];
      _revenues = new double[_nationcard];
      Vec.Reader s_nationkey = _s_nationkey.new Reader();
      
      for( int i=0; i<orderkeys._len; i++ ) {
        Integer nationkey = _orderkeys.get(orderkeys.at8(i));
        if( nationkey==null ) continue;
        int nkey = nationkey;
        if( s_nationkey.at8(suppkeys.at8(i)-1) != nkey ) continue;
        _revenues[nkey] += extendprs.atd(i)*(1.0-discounts.atd(i));
      }
    }
    @Override public void reduce( NRCOL bld ) {
      ArrayUtils.add(_revenues,bld._revenues);
    }
  }














  // ------------------------------------------------------------------------------
  // Old Query plan:

  // Filter ORDERS by year-date.
  // Filter NATION_REGION_SUPPLIER by REGION
  // Join NRS with CUSTOMER by n_name
  // Join NRSC with ORDERS by custkey (most expensive step)
  // Compute Small data unique orderkeys, and nation-per-suppkey-per-orderkey.
  //   Works because unique orderkeys is about 1/400 NRSC; should work up to SF100.
  //   Avoids a final Join of NRSCO with LINEITEM - which is far more expensive.
  // Filter LINEITEM by matching orderkey+suppkey; GroupBy nation & compute revenue.

  public Frame run_prior() {
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
