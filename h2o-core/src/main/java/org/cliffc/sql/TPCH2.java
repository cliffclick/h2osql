package org.cliffc.sql;

import water.MRTask;
import water.fvec.*;
import water.util.ArrayUtils;
import water.rapids.Merge;
import water.nbhm.NonBlockingHashMapLong;

import java.util.BitSet;

/**
The Minimum Cost Supplier Query finds, in a given region, for each part of a
certain type and size, the supplier who can supply it at minimum cost. If
several suppliers in that region offer the desired part type and size at the
same (minimum) cost, the query lists the parts from suppliers with the 100
highest account balances. For each supplier, the query lists the supplier's
account balance, name and nation; the part's number and manufacturer; the
supplier's address, phone number and comment information.

select
    s_acctbal,
    s_name,
    n_name,
    p_partkey,
    p_mfgr,
    s_address,
    s_phone,
    s_comment
from
    part,
    supplier,
    partsupp,
    nation,
    region
where
    p_partkey = ps_partkey
    and s_suppkey = ps_suppkey
    and p_size = @@1
    and p_type like '%@@2'
    and s_nationkey = n_nationkey
    and n_regionkey = r_regionkey
    and r_name = '@@3'
    and ps_supplycost = (
        select
            min(ps_supplycost)
        from
            partsupp,
            supplier,
            nation,
            region
        where
            p_partkey = ps_partkey
            and s_suppkey = ps_suppkey
            and s_nationkey = n_nationkey
            and n_regionkey = r_regionkey
            and r_name = '@@3'
    )
order by
    s_acctbal desc,
    n_name,
    s_name,
    p_partkey;

Validation, SF=0.01
4186.95|Supplier#000000077 |GERMANY | 249|Manufacturer#4 |wVtcr0uH3CyrSiWMLsqnB09Syo,UuZxPMeBghlY|17-281-345-4863 |the slyly final asymptotes. blithely pending theodoli
1883.37|Supplier#000000086 |ROMANIA |1015|Manufacturer#4 |J1fgg5QaqnN                            |29-903-665-7065 |cajole furiously special, final requests: furiously spec
1687.81|Supplier#000000017 |ROMANIA |1634|Manufacturer#2 |c2d,ESHRSkK3WYnxpgw6aOqN0q             |29-601-884-9219 |eep against the furiously bold ideas. fluffily bold packa
 287.16|Supplier#000000052 |ROMANIA | 323|Manufacturer#4 |WCk XCHYzBA1dvJDSol4ZJQQcQN,           |29-974-934-4713 |dolites are slyly against the furiously regular packages. ironic, final deposits cajole quickly

Validation, SF=1
Frame null (460 rows and 8 cols):
                    acctbal              s_name          n_name                               s_address            phone                                                                                      s_comment            partkey            mfgr
      0             9938.53  Supplier#000005359  UNITED KINGDOM                QKuHYh,vZGiwu2FWEJoLDx04  33-429-790-6131                                                                  uriously regular requests hag             185358  Manufacturer#4
      1             9937.84  Supplier#000005969         ROMANIA       ANDENSOSmk,miq23Xfb5RWt6dvUcvt6Qa  29-520-692-3537                            efully express instructions. regular requests against the slyly fin             108438  Manufacturer#1
      2             9936.22  Supplier#000005250  UNITED KINGDOM                   B3rqp0xbSEim4Mpy2RH J  33-320-228-2957  etect about the furiously final accounts. slyly ironic pinto beans sleep inside the furiously                249  Manufacturer#4
      3             9923.77  Supplier#000002324         GERMANY                            y3OD9UywSTOk  17-779-299-1839                                            ackages boost blithely. blithely regular deposits c              29821  Manufacturer#4
      4             9871.22  Supplier#000006373         GERMANY                              J8fcXWsTqM  17-813-485-8637                 etect blithely bold asymptotes. fluffily ironic platelets wake furiously; blit              43868  Manufacturer#5
      5             9870.78  Supplier#000001286         GERMANY   YKA,E2fjiVd7eUrzp2Ef8j1QxGo2DFnosaTEH  17-516-924-4574                                        regular accounts. furiously unusual courts above the fi              81285  Manufacturer#2
      6             9870.78  Supplier#000001286         GERMANY   YKA,E2fjiVd7eUrzp2Ef8j1QxGo2DFnosaTEH  17-516-924-4574                                        regular accounts. furiously unusual courts above the fi             181285  Manufacturer#4
      7             9852.52  Supplier#000008973          RUSSIA                t5L67YdBYYH6o,Vz24jpDyQ9  32-188-594-7038                                                rns wake final foxes. carefully unusual depende              18972  Manufacturer#2
      8             9847.83  Supplier#000008097          RUSSIA                       xMe97bpE69NzdwLoX  32-375-640-3593                                the special excuses. silent sentiments serve carefully final ac             130557  Manufacturer#2
      9             9847.57  Supplier#000006345          FRANCE  VSt3rzk3qG698u6ld8HhOByvrTcSTSvQlDQDag  16-886-766-7945      ges. slyly regular requests are. ruthless, express excuses cajole blithely across the unu              86344  Manufacturer#1

      SF0.01  SF1    SF10
Umbra         0.011
H2O   0.003   0.012  0.50
*/

public class TPCH2 implements SQL.TPCH {
  @Override public String name() { return "TPCH2"; }
  static final boolean PRINT_TIMING = false;
  static final int SIZE=15;
  static final String TYPE="BRASS";
  static final String REGION="EUROPE";

  static final String[] PARTCOLS = new String[]{"partkey", "mfgr",    "type",    "size"};
  static final int                               PARTIDX=0 ,MFGRIDX=1, TYPEIDX=2, SIZEIDX=3;

  // Query Plan:
  // - hash/join NATION + REGION on regionkey; filter REGION by EUROPE; card==5; result is bitset of nationkeys
  // - hash/join N_R + SUPPLIER on nationkey; card==2k; result is map suppkey->{acctbal,s_name}
  // - hash/join PART + PARTSUPP on partkey; filter PART by SIZE & TYPE; card=7k; result is set of {suppkey,partkey,ps_supplycost}
  // - hash/join P_PS + N_R_S on suppkey; result is {partkey,ps_supplycost,acctbal,s_name}; card=1k
  // - hash/join PARTSUPP + N_R_S on suppkey; filter partkey exists; card=160k; result is map partkey->{ps_supplycost}
  //   - groupby on partkey, min of ps_supplycost; card=144k
  // - hash/join P_PS_N_R_S + GrpBy; filter min[partkey]==ps_supplycost; card=1k
  // - Format
  
  @Override public Frame run() {
    long t0 = System.currentTimeMillis();

    // - hash/join NATION+REGION on regionkey; filter REGION by EUROPE; card==5
    // Results in a set of nationkeys.
    int regionkey = ArrayUtils.find(SQL.REGION.frame().vec("r_name").domain(),REGION);
    Frame nation = SQL.NATION.frame();
    Vec.Reader nation_r_name = nation.vec("r_name").new Reader();
    BitSet nationkeys = new BitSet();
    for( int i=0; i<nation_r_name.length(); i++ )
      if( nation_r_name.at8(i) == regionkey )
        nationkeys.set(i);
    if( PRINT_TIMING ) { long t=System.currentTimeMillis(); System.out.println("NR#"+(nationkeys.cardinality())+", "+(t-t0)+" msec"); t0=t; }

    // - hash/join N_R + SUPPLIER on nationkey; card==20k; result is set of suppkeys.
    Frame supplier = SQL.SUPPLIER.frame();
    Vec s_n_name = supplier.vec("n_name");
    NonBlockingHashMapLong suppkeys = new NRS(nationkeys).doAll(s_n_name)._suppkeys;
    if( PRINT_TIMING ) { long t=System.currentTimeMillis(); System.out.println("NRS#"+(suppkeys.size())+", "+(t-t0)+" msec"); t0=t; }

    // Compute the categoricals containing TYPE
    Frame part = SQL.PART.frame();
    String[] types = part.vec("type").domain();
    boolean[] is_type = new boolean[types.length];
    for( int i=0; i<types.length; i++ )
      is_type[i] = types[i].contains(TYPE);
    
    // - Filter PART by SIZE & TYPE; card=2k; result is a set of partkeys
    // Filter out unexciting part columns; keep whats needed the query.
    NonBlockingHashMapLong partkeys = new FilterPart2(is_type).doAll(part.vec("type"),part.vec("size"))._partkeys;
    if( PRINT_TIMING ) { long t=System.currentTimeMillis(); System.out.println("PARTS#"+(partkeys.size())+", "+(t-t0)+" msec"); t0=t; }

    // - hash/join PARTSUPP + N_R_S on suppkey; filter by partkey also (reduces resulting map by 250x);
    //   - groupby on partkey, min of ps_supplycost; card=144k
    //   Result is map partkey->{min ps_supplycost}
    Frame partsupp = SQL.PARTSUPP.frame();
    Frame partsupp1 = partsupp.subframe(new String[]{"partkey", "suppkey","supplycost"});
    NonBlockingHashMapLong<Long> mins = new MinCost2(partkeys,suppkeys).doAll(partsupp1)._mins;
    if( PRINT_TIMING ) { long t=System.currentTimeMillis(); System.out.println("MINCOST#"+(mins.size())+", "+(t-t0)+" msec"); t0=t; }

    // Format
    // Deep-slice the suppliers by selected columns and rows
    int len = mins.size(), row=0;
    long[] xrows = new long[len];
    for( Long pack : mins.values() )   xrows[row++] = pack2suppkey(pack)-1;
    long[] xcols = new long[]{ supplier.find("acctbal"  ), supplier.find("s_name"), supplier.find("n_name"),
                               supplier.find("s_address"), supplier.find("phone" ), supplier.find("s_comment") };
    Frame fr = supplier.deepSlice(xrows,xcols);
    // Add matching partkey,mfgr
    Vec p_mfgr = part.vec("mfgr");
    Vec.Reader vrp_mfgr = p_mfgr.new Reader();
    row=0;
    long[] p_pkeys = new long[len];
    long[] p_mfgrs = new long[len];
    for( long partkey : mins.keySetLong() ) {
      p_pkeys[row  ] = partkey;
      p_mfgrs[row++] = vrp_mfgr.at8(partkey-1);
    }
    fr.add("partkey",Vec.makeVec(p_pkeys ,null             ,Vec.newKey()));
    fr.add("mfgr"   ,Vec.makeVec(p_mfgrs ,p_mfgr  .domain(),Vec.newKey()));
    // Sort by acctbal
    Frame rez = Merge.sort(fr,fr.find(new String[]{"acctbal",       "n_name",       "s_name",       "partkey"}),
                                      new int[]   {Merge.DESCENDING,Merge.ASCENDING,Merge.ASCENDING,Merge.ASCENDING});
    fr.delete();
    if( PRINT_TIMING ) { long t=System.currentTimeMillis(); System.out.println("Format#"+(rez.numRows())+", "+(t-t0)+" msec"); t0=t; }
    
    return rez;
  }

  // Filter suppliers by nation.  Result is a set of suppliers.
  private static class NRS extends MRTask<NRS> {
    transient NonBlockingHashMapLong _suppkeys;
    final BitSet _nationkeys;
    NRS( BitSet nationkeys ) { _nationkeys = nationkeys; }
    @Override protected void setupLocal() { _suppkeys = new NonBlockingHashMapLong(); }
    @Override public void map( Chunk n_names ) {
      long start = n_names.start();
      for( int i=0; i<n_names._len; i++ ) {
        int nationkey = (int)n_names.at8(i);
        if( _nationkeys.get(nationkey) )
          _suppkeys.put(start+i+1/*(long)suppkeys.at8(i)*/,"");
      }
    }
    @Override public void reduce( NRS bld ) {
      if( _suppkeys != bld._suppkeys )
        throw new RuntimeException("distributed reduce not implemented");
    }
  }

  // Filter PART by TYPE and SIZE.  Reduces dataset by ~50x.
  private static class FilterPart2 extends MRTask<FilterPart2> {
    transient NonBlockingHashMapLong _partkeys;
    final boolean[] _is_type;
    FilterPart2(boolean[] is_type) { _is_type = is_type; }
    @Override protected void setupLocal() { _partkeys = new NonBlockingHashMapLong(); }
    @Override public void map( Chunk types, Chunk sizes ) {
      long start = types.start();
      for( int i=0; i<types._len; i++ )
        if( sizes.at8(i)==SIZE && _is_type[(int)types.at8(i)] )
          _partkeys.put(start+i+1/*(long)partkeys.at8(i)*/,"");
    }
    @Override public void reduce( FilterPart2 bld ) {
      if( _partkeys != bld._partkeys )
        throw new RuntimeException("distributed reduce not implemented");
    }
  }

  // Walk PARTSUPP, filter by suppliers from the correct nations and by
  // partkeys that match the size & type; group-by partkey and find the min
  // ps_supplycost.  Result is a map->{min(packed supplycost+suppkey)}.
  // Filtering by parts also reduces the result map by 250x.
  private static class MinCost2 extends MRTask<MinCost2> {
    transient NonBlockingHashMapLong<Long> _mins;
    final NonBlockingHashMapLong _partkeys;
    final NonBlockingHashMapLong _suppkeys;
    MinCost2( NonBlockingHashMapLong partkeys, NonBlockingHashMapLong suppkeys ) { _partkeys=partkeys; _suppkeys=suppkeys; }
    @Override protected void setupLocal() { _mins = new NonBlockingHashMapLong<>(); }
    @Override public void map( Chunk[] cs ) {
      Chunk partkeys = cs[0];
      Chunk suppkeys = cs[1];
      Chunk supcosts = cs[2];
      // The Main Hot Loop
      for( int i=0; i<suppkeys._len; i++ ) {
        long suppkey = suppkeys.at8(i);
        long partkey = partkeys.at8(i);
        if( _partkeys.containsKey(partkey) && _suppkeys.containsKey(suppkey) ) {
          double cost  = supcosts.atd(i);
          Long pack = _mins.get(partkey);
          if( pack==null || cost<pack2suppcost(pack) )
            _mins.put(partkey,(Long)suppkey2pack(suppkey,cost));
        }
      }
    }
    @Override public void reduce(MinCost2 mc) {
      if( _mins != mc._mins )
        throw new RuntimeException("distributed reduce not implemented");
    }
  }
  // Pack a suppkey & suppcost in a long
  private static long suppkey2pack( long suppkey, double cost ) {
    int costi = (int)(cost*100+0.5);
    assert Math.abs(((double)costi)/100.0-cost) < 1e-10 : "Cost does not pack: "+cost+", costi="+costi+", err="+(Math.abs(((double)costi)/100.0-cost));
    int suppi = (int)suppkey;
    assert suppi==suppkey;
    return (((long)suppi)<<32)|costi;
  }
  // Unpack a suppkey from a packed suppkey
  private static int pack2suppkey( long pack ) { return (int)(pack>>32); }
  private static double pack2suppcost( long pack ) { return ((double)((int)pack))/100; }
 
  
  // Filter suppliers by nation.  Result is a set of suppliers.
  private static class P_PS extends MRTask<P_PS> {
    final NonBlockingHashMapLong _partkeys;
    final NonBlockingHashMapLong<Double> _mins;
    final NonBlockingHashMapLong _suppkeys;
    P_PS( NonBlockingHashMapLong partkeys, NonBlockingHashMapLong suppkeys, NonBlockingHashMapLong<Double> mins ) {
      _partkeys=partkeys;  _suppkeys=suppkeys;  _mins=mins;
    }
    @Override public void map( Chunk[] cs, NewChunk[] ncs ) {
      Chunk partkeys = cs[0];
      Chunk suppkeys = cs[1];
      Chunk supcosts = cs[2];
      for( int i=0; i<partkeys._len; i++ ) {
        long partkey = partkeys.at8(i);
        long suppkey = suppkeys.at8(i);
        if( _partkeys.containsKey(partkey) && _suppkeys.containsKey(suppkey) ) {
          double cost = supcosts.atd(i);
          Double mincost = _mins.get(partkey);
          if( mincost!=null && mincost==cost )
            SQL.copyRow(cs,ncs,i);
        }
      }
    }
  }
  
}
