package org.cliffc.sql;

import water.*;
import water.fvec.*;
import water.util.ArrayUtils;
import water.rapids.Merge;
import water.nbhm.NonBlockingHashMapLong;
import org.joda.time.DateTime;
import java.util.Arrays;

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
*/

public class Query2 implements SQL.Query {
  @Override public String name() { return "Query2"; }
  static final int SIZE=15;
  static final String TYPE="BRASS";
  static final String REGION="EUROPE";

  static boolean[] IS_TYPE;     // Which categoricals contain the TYPE string
  static final String[] PARTCOLS = new String[]{"partkey", "mfgr",    "type",    "size"};
  static final int                               PARTIDX=0 ,MFGRIDX=1, TYPEIDX=2, SIZEIDX=3;

  // Query plan:
  // Filter parts by SIZE and TYPE.  This is like a 5%-pass filter.
  // JOIN filtered parts with [nation,region,supplier,partsupp] on partkey
  // Find min partsupp.supplycost grouped-by part - coult move the region filter here.
  // Filter again by min supplycost-per-part and region.
  // Sort by supplier.acctbal/nation/supplier/partkey; report top 100.
  
  @Override public Frame run() {
    // Filter out unexciting part columns; keep whats needed for reporting and the query.
    Frame part0 = SQL.PART.frame();
    Frame part1 = part0.subframe(PARTCOLS);
    
    // Compute the categoricals containing TYPE
    String[] types = part1.vec("type").domain();
    boolean[] is_type = new boolean[types.length];
    for( int i=0; i<types.length; i++ )
      is_type[i] = types[i].contains(TYPE);

    // Filter parts to matching SIZE and TYPE
    int[] p3cols = new int[]{PARTIDX,MFGRIDX};
    Frame part2 = new FilterPart(is_type).doAll(part1.types(p3cols),part1).outputFrame(part1.names(p3cols),part1.domains(p3cols));
    // Repack the (very) sparse result into fewer chunks
    Frame part3 = SQL.compact(part2);

    // Filter the big join to just the exciting columns
    Frame bigjoin = SQL.NATION_REGION_SUPPLIER_PARTSUPP.subframe(MINCOLS);
    
    // JOIN with NATION_REGION_SUPPLIER_PARTSUPP
    Frame partsjoin = SQL.join(bigjoin,part3);
    part3.delete();
    
    // Find the minimum supplycost-per-part.  Treated as small data.
    int region = ArrayUtils.find(partsjoin.vec("r_name").domain(),REGION);
    NonBlockingHashMapLong<Double> mins = new MinCost(region).doAll(partsjoin)._mins;
    
    // Filter again to matching supplycost & region.  Keep the first 8 cols
    // only, dropping r_name and supplycost.
    Vec r_name = partsjoin.remove("r_name");
    Vec supplycost = partsjoin.remove("supplycost");
    partsjoin.add("r_name",r_name); // Shuffle to the end, to drop them
    partsjoin.add("supplycost",supplycost);
    int[] r0cols = new int[]{PARTIDX,1,2,3,4,5,6,7};
    Frame rez0 = new FilterCost(region,mins).doAll(partsjoin.types(r0cols),partsjoin).outputFrame(partsjoin.names(r0cols),partsjoin.domains(r0cols));
    partsjoin.delete();
    // Repack the sparse result into fewer chunks
    Frame rez1 = SQL.compact(rez0);
    Frame rez2 = rez1.subframe(new String[]{"acctbal","s_name","n_name","partkey","mfgr","s_address","phone","s_comment"});

    // Sort
    Frame rez3 = Merge.sort(rez2,rez2.find(new String[]{"acctbal",       "n_name",       "s_name",       "partkey"}),
                                           new int[]   {Merge.DESCENDING,Merge.ASCENDING,Merge.ASCENDING,Merge.ASCENDING});
    rez2.delete();
    return rez3;
  }

  // Filter by TYPE and SIZE.  Reduces dataset by ~50x
  private static class FilterPart extends MRTask<FilterPart> {
    final boolean[] _is_type;
    FilterPart(boolean[] is_type) { _is_type = is_type; }
    @Override public void map( Chunk[] cs, NewChunk[] ncs ) {
      Chunk parts = cs[PARTIDX], mfgrs = cs[MFGRIDX];
      Chunk types = cs[TYPEIDX], sizes = cs[SIZEIDX];
      NewChunk nparts = ncs[PARTIDX], nmfgrs = ncs[MFGRIDX];
      // The Main Hot Loop
      for( int i=0; i<types._len; i++ )
        if( _is_type[(int)types.at8(i)] && sizes.at8(i)==SIZE ) {
          nparts.addNum(parts.at8(i)); 
          nmfgrs.addNum(mfgrs.at8(i));
        }
    }
  }

  // Find min-cost supply amongst unique parts.

  static final String[] MINCOLS = new String[]{"partkey","acctbal","s_name","n_name","s_address","phone","s_comment","r_name","supplycost"};
  static final int REGIONIDX=7, SUPCOSTIDX=8;
  { assert MINCOLS[REGIONIDX].equals("r_name")
      &&   MINCOLS[  PARTIDX].equals("partkey");
  }
  private static class MinCost extends MRTask<MinCost> {
    final int _region;
    NonBlockingHashMapLong<Double> _mins;
    MinCost( int region ) { _region=region; }
    @Override public void map( Chunk[] cs ) {
      Chunk partkeys= cs[PARTIDX];
      Chunk costs   = cs[SUPCOSTIDX];
      Chunk r_names = cs[REGIONIDX];
      _mins = new NonBlockingHashMapLong<>();
      // The Main Hot Loop
      for( int i=0; i<costs._len; i++ ) {
        if( r_names.at8(i)==_region ) {
          long partkey = partkeys.at8(i);
          double cost = costs.atd(i);
          Double min = _mins.get(partkey);
          if( min==null || cost<min )
            _mins.put(partkey,(Double)cost);
        }
      }
    }
    @Override public void reduce(MinCost mc) {
      for( long partkey : mc._mins.keySetLong() ) {
        Double d0 =    _mins.get(partkey);
        Double d1 = mc._mins.get(partkey);
        if( d0==null || d0 < d1 )
          _mins.put(partkey,d1);
      }
    }
  }  

  private static class FilterCost extends MRTask<FilterCost> {
    final int _region;
    final NonBlockingHashMapLong<Double> _mins;
    FilterCost( int region, NonBlockingHashMapLong<Double> mins ) { _region = region; _mins = mins; }
    @Override public void map( Chunk[] cs, NewChunk[] ncs ) {
      Chunk partkeys= cs[PARTIDX];
      Chunk costs   = cs[SUPCOSTIDX+1];
      Chunk r_names = cs[REGIONIDX+1];
      // The Main Hot Loop
      for( int i=0; i<costs._len; i++ )
        if( r_names.at8(i)==_region ) {
          long partkey = partkeys.at8(i);
          double cost = costs.atd(i);
          Double min = _mins.get(partkey);
          if( (double)min==cost )
            SQL.copyRow(cs,ncs,i);
        }
    }
  }
  
}
