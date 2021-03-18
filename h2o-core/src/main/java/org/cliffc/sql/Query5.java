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

public class Query5 implements SQL.Query {
  @Override public String name() { return "Query5"; }
  static final boolean PRINT_TIMING = true;
  
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
    Frame nrsco_1 = nrsco_0.subframe(new String[]{"orderkey","suppkey","n_name"}); // drop custkey, gain orderkey
    long t_join1 = System.currentTimeMillis();
    if( PRINT_TIMING ) System.out.print("join1 "+(t_join1-t)+" msec, "); t=t_join1;

    // LineItems
    Frame line0 = SQL.LINEITEM.frame();
    Frame line1 = line0.subframe(new String[]{"orderkey","suppkey","extendedprice","discount"});
    
    //// Join NRSCO with lineitem using orderkey,suppkey
    //Frame nrscol_0 = SQL.join(nrsco_1,line1);
    //nrsco_0.delete();
    //long t_join2 = System.currentTimeMillis();
    //if( PRINT_TIMING ) System.out.print("join2 "+(t_join2-t)+" msec, "); t=t_join2;    
    //// GroupBy n_name, computing revenue
    //double[] n_revs = new GBRevenue().doAll(nrscol_0.vec("n_name"),nrscol_0.vec("extendedprice"),nrscol_0.vec("discount")).n_revs;

    // Gather suppkeys-per-unique-ord.  Will hash on orderkey, and linear search on suppkey
    NonBlockingHashMapLong<int[]> uniqords = new UniqueOrders().doAll(nrsco_1.vecs())._uniqords;
    long t_uniq = System.currentTimeMillis();
    if( PRINT_TIMING ) System.out.print("uniques "+(t_uniq-t)+" msec, "); t=t_uniq;

    // Filter out non-matching order/supp pairs.
    // GroupBy nation and compute revenue.
    double[] n_revs = new FilterLNGB(nrs0.vec("n_name").cardinality(),uniqords).doAll(line1).n_revs;    
    long t_gb = System.currentTimeMillis();
    if( PRINT_TIMING ) System.out.print("GB "+(t_gb-t)+" msec, "); t=t_gb;

    // got customers+nation+region+suppliers, with same nation & region==ASIA, picks up custkey
    // JOIN1: NRSC & ORDS3 (pre-filtered by year)

    // already ordsupp joined on 
    // expensive join: check lineitem.orderkey == ordsupp.orderkey && lineitem.suppkey == ordsupp.suppkey
    // want: just filtered lineitems with test (lineitem.orderkey == ordsupp.orderkey && lineitem.suppkey == ordsupp.suppkey),
    // GB on nation w/revenue.

    // is orderkey/suppkey small?  used to filter lineitems.
    // ORDERS=1.5M*SF == 4bytes.  SUPPLIERS is small.  Can be 8-bytes.
    // 18M rows in SF1, 180M in SF10, 1800M in SF100.
    // Use NBHML?  8-bytes for order/spp, reports back missing or nationkey.
    // Barely fit SF10 (180M entries) in a single 2G array.
    
    // Orderkeys are sorted in orders.  are they sorted in NRSC?
    // can i binsearch orderkeys in lineitem?;
    // - from matching row filter out mismatch suppkey
    // - - from matching suppkey, get nation & compute GB rev

    // NOPE...
    // dense array of suppkey & nation; index by orderkey.
    // Limit of 2B orderkeys fits SF100; supkey is 10K*SF; SF1=> 2bytes, SF10=>100K=>2bytes+1bit, SF100==>1M=>20bits/3bytes.
    // Fits SF 100 with 4byte suppkeys.  Nation is a byte.
    // orderkey is NOT unique; several matching suppliers

    // Plan C: unique orderkeys is 400x smaller, and each orderkeys has 4-6
    // matching suppkey & nation.  NBHML orderkeys to int[]/pair of suppkey/
    // nationkey, growable array.
    

    
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
    transient NonBlockingHashMapLong<int[]> _uniqords;
    @Override protected void setupLocal() { _uniqords = new NonBlockingHashMapLong<>(10000); }
    @Override public void map(Chunk ordkeys, Chunk suppkeys, Chunk nnames) {
      for( int i=0; i<ordkeys._len; i++ ) {
        long ordkey =     ordkeys .at8(i);
        int suppkey= (int)suppkeys.at8(i);
        int nname  = (int)nnames  .at8(i);
        while( true ) {
          int[] sns = _uniqords.get(ordkey);
          if( sns==null ) {
            if( _uniqords.putIfAbsent(ordkey,new int[]{suppkey,nname}) == null ) break;
          } else {
            int[] nnn = Arrays.copyOf(sns,sns.length+2);
            nnn[sns.length  ]=suppkey;
            nnn[sns.length+1]=nname;
            if( _uniqords.replace(ordkey,sns,nnn) ) break;
          }
        }
      }
    }
    @Override public void reduce( UniqueOrders uqo ) {
      if( _uniqords != uqo._uniqords )
        throw new RuntimeException("distributed reduce not implemented");
    }
  }

  private static class FilterLNGB extends MRTask<FilterLNGB> {
    final NonBlockingHashMapLong<int[]> _uniqords;
    final int _nation_card;
    double[] n_revs;
    FilterLNGB( int nation_card, NonBlockingHashMapLong<int[]> uniqords ) {
      _uniqords = uniqords;
      _nation_card = nation_card;
    }
    @Override public void map( Chunk[] cs ) {
      Chunk l_ordks = cs[0], l_supks = cs[1], exprices = cs[2], discs = cs[3];
      n_revs = new double[_nation_card]; // revenue-by-nation
      for( int i=0; i<l_ordks._len; i++ ) {
        long ordkey = l_ordks.at8(i);
        int[] supns = _uniqords.get(ordkey);
        if( supns==null ) continue; // No matching order
        int suppkey = (int)l_supks.at8(i);
        for( int j=0; j<supns.length; j+=2 )
          if( supns[j]==suppkey ) {
            n_revs[supns[j+1]] += exprices.atd(i)*(1.0-discs.atd(i));
            break;
          }
      }
    }
    @Override public void reduce( FilterLNGB flngb ) {
      ArrayUtils.add(n_revs,flngb.n_revs);
    }
  }

  
  private static class AssertSort extends MRTask<AssertSort> {
    @Override public void map(Chunk oks) {
      for( int i=0; i<oks._len-1; i++ )
        assert oks.at8(i)<oks.at8(i+1);
    }
  }

  // Failed because orderkeys are not unique
//    // Fill suppkey/nationkey array, indexed by orderkey.
//    int nrows = (int)nrsco_1.vec("orderkey").max()+1;
//    int[] suppkeys = new int[nrows];
//    byte[] nkeys  = new byte[nrows];
//    new FillSN(suppkeys,nkeys).doAll(nrsco_1.vecs());
//    double[] n_revs = new FilterLNGB(nrs0.vec("n_name").cardinality(),suppkeys,nkeys).doAll(line1).n_revs;
//  static class FillSN extends MRTask<FillSN> {
//    final int[] _suppkeys;
//    final byte[] _nkeys;
//    FillSN(int[] suppkeys, byte[] nkeys) { _suppkeys=suppkeys; _nkeys=nkeys; }
//    @Override public void map( Chunk ordkeys, Chunk suppkeys, Chunk nkeys ) {
//      for( int i=0; i<ordkeys._len; i++ ) {
//        int ordkey = (int)ordkeys.at8(i);
//        assert _suppkeys[ordkey]==0; // fails
//        _suppkeys[ordkey] = (int) suppkeys.at8(i);
//        _nkeys   [ordkey] = (byte)nkeys   .at8(i);
//      }
//    }
//  }
//  static class FilterLNGB extends MRTask<FilterLNGB> {
//    final int[] _suppkeys;
//    final byte[] _nkeys;
//    final int _nation_card;
//    double[] n_revs;
//    FilterLNGB( int nation_card, int[] suppkeys, byte[] nkeys ) {
//      _suppkeys = suppkeys;
//      _nkeys = nkeys;
//      _nation_card = nation_card;
//    }
//    @Override public void map( Chunk[] cs ) {
//      Chunk l_ordks = cs[0], l_supks = cs[1], exprices = cs[2], discs = cs[3];
//      n_revs = new double[_nation_card];
//      for( int i=0; i<l_ordks._len; i++ ) {
//        int ordkey = (int)l_ordks.at8(i);
//        if( ordkey < _suppkeys.length && l_supks.at8(i)== _suppkeys[ordkey] )
//          n_revs[_nkeys[ordkey]] += exprices.atd(i)*(1.0-discs.atd(i));
//      }
//    }
//    @Override public void reduce( FilterLNGB flngb ) {
//      ArrayUtils.add(n_revs,flngb.n_revs);
//    }
//  }

//  static class FilterLNGB extends MRTask<FilterLNGB> {
//    final Vec _vordkeys, _vsuppkeys, _vnnames;
//    final int _nation_card;
//    double[] n_revs;
//    FilterLNGB( Frame nrcso ) {
//      _vordkeys = nrcso.vec("orderkey");
//      _vsuppkeys= nrcso.vec("suppkey");
//      _vnnames  = nrcso.vec("n_name");
//      _nation_card = _vnname.cardinality();
//    }
//    @Override public void map( Chunk[] cs ) {
//      Chunk l_ordks = cs[0], l_supks = cs[1], exprices = cs[2], discs = cs[3];
//      n_revs = new double[_nation_card];
//      Reader vrord = _vordkeys.new Reader();
//      for( int i=0; i<l_ordks._len; i++ ) {
//        long ordkey = l_ordks.at8(i);
//        int row = binsearch(vrod,ordkey);
//        if( row==-1 ) continue; // No matching orderkey
//        assert vrord.at8(row)==ordkey;
//        long suppkey = l_supks.at8(i);
//        while( vrord.at8(row)==ordkey ) {
//          if( suppkey == vrsupp.at8(row) ) {
//            n_revs[vrnkey.at8(row)] += exprices.atd(i)*(1.0-discs.atd(i));
//            break;              // the next row will not match both ord & supp or else we got dups
//          }
//          row++;
//        }
//      }
//    }
//    @Override public void reduce( FilterLNGB flngb ) {
//      ArrayUtils.add(n_revs,flngb.n_revs);
//    }
//  }


//    // Fill suppkey/nationkey array, indexed by orderkey.
//    int nrows = (int)nrsco_1.vec("orderkey").max()+1;
//    int[] suppkeys = new int[nrows];
//    byte[] nkeys  = new byte[nrows];
//    new FillSN(suppkeys,nkeys).doAll(nrsco_1.vecs());
//    double[] n_revs = new FilterLNGB(nrs0.vec("n_name").cardinality(),suppkeys,nkeys).doAll(line1).n_revs;
//  static class FillSN extends MRTask<FillSN> {
//    final int[] _suppkeys;
//    final byte[] _nkeys;
//    FillSN(int[] suppkeys, byte[] nkeys) { _suppkeys=suppkeys; _nkeys=nkeys; }
//    @Override public void map( Chunk ordkeys, Chunk suppkeys, Chunk nkeys ) {
//      for( int i=0; i<ordkeys._len; i++ ) {
//        int ordkey = (int)ordkeys.at8(i);
//        assert _suppkeys[ordkey]==0; // fails
//        _suppkeys[ordkey] = (int) suppkeys.at8(i);
//        _nkeys   [ordkey] = (byte)nkeys   .at8(i);
//      }
//    }
//  }


  
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
