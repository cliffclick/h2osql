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

public class Query2 {
  static final int SIZE=15;
  static final String TYPE="BRASS";
  static final String REGION="EUROPE";

  static boolean[] IS_TYPE;     // Which categoricals contain the TYPE string

  // JOIN: [part] and [partsupp] on partkey
  // JOIN: [part,partsupp] and [supplier] on suppkey
  // JOIN: [part,partsupp,supplier] and [nation] on nationkey
  // JOIN: [part,partsupp,supplier,nation] and [region] on regionkey
  // JOIN: [part,partsupp,supplier,nation,region]

  // Match the following
  // [part].size==SIZE &&
  // [part].type.like(TYPE) &&
  // [region].name==REGION &&

  // Find min:
  // ps_supplycost = min(partsupp.supplycost)

  // Filter again:
  // [partsupp].supplycost == ps_supplycost
  
  // Sort by supplier.acctbal/nation/supplier/partkey; report top 100.
  
  public static Frame run() {
    // Filter out unexciting part columns; keep whats needed for reporting and the query.
    Frame part0 = SQL.PART.frame();
    Frame part1 = part0.subframe(new String[]{"partkey","mfgr","type","size"});
    
    // Compute the categoricals containing TYPE
    String[] types = part1.vec("type").domain();
    boolean[] is_type = new boolean[types.length];
    for( int i=0; i<types.length; i++ )
      is_type[i] = types[i].contains(TYPE);

    // Filter parts to matching SIZE and TYPE
    Frame part2 = new FilterPart(part1,is_type).doAll(part1.types(),part1).outputFrame(part1.names(),part1.domains());
    // Repack the (very) sparse result into fewer chunks
    Frame part3 = SQL.compact(part2);

    // Filter the big join to just the exciting columns
    Frame bigjoin = SQL.NATION_REGION_SUPPLIER_PARTSUPP.subframe(new String[]{"acctbal","s_name","n_name","s_address","phone","s_comment","r_name","partkey","supplycost"});
    
    // JOIN with NATION_REGION_SUPPLIER_PARTSUPP
    Frame partsjoin = SQL.join(bigjoin,part3);
    part3.delete();
    
    // Find the minimum supplycost-per-part.  Treated as small data.
    NonBlockingHashMapLong<Double> mins = new MinCost(partsjoin).doAll(partsjoin)._mins;
    
    // Filter again to matching supplycost & region
    Frame rez0 = new FilterCost(partsjoin,mins).doAll(partsjoin.types(),partsjoin).outputFrame(partsjoin.names(),partsjoin.domains());
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
    final int _type_idx, _size_idx;
    final boolean[] _is_type;
    FilterPart(Frame fr, boolean[] is_type) {
      _type_idx = fr.find("type");
      _size_idx = fr.find("size");
      _is_type = is_type;
    }
    @Override public void map( Chunk[] cs, NewChunk[] ncs ) {
      Chunk type = cs[_type_idx];
      Chunk size = cs[_size_idx];
      // The Main Hot Loop
      for( int i=0; i<type._len; i++ )
        if( _is_type[(int)type.at8(i)] && size.at8(i)==SIZE )
          SQL.copyRow(cs,ncs,i);
    }
  }

  // Find min-cost supply amongst unique parts.
  private static class MinCost extends MRTask<MinCost> {
    final int _partkey_idx, _cost_idx, _r_name_idx;
    final int _region_cat;
    NonBlockingHashMapLong<Double> _mins;

    MinCost( Frame fr ) {
      _partkey_idx= fr.find("partkey");
      _cost_idx   = fr.find("supplycost");
      _r_name_idx = fr.find("r_name");
      _region_cat = ArrayUtils.find(fr.vec(_r_name_idx).domain(),REGION);
    }
    @Override public void map( Chunk[] cs ) {
      Chunk partkeys= cs[_partkey_idx];
      Chunk costs   = cs[_cost_idx];
      Chunk r_names = cs[_r_name_idx];
      _mins = new NonBlockingHashMapLong<>();
      // The Main Hot Loop
      for( int i=0; i<costs._len; i++ ) {
        if( r_names.at8(i)==_region_cat ) {
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
    final int _partkey_idx, _cost_idx, _r_name_idx;
    final int _region_cat;
    final NonBlockingHashMapLong<Double> _mins;
    FilterCost( Frame fr, NonBlockingHashMapLong<Double> mins ) {
      _partkey_idx= fr.find("partkey");
      _cost_idx   = fr.find("supplycost");
      _r_name_idx = fr.find("r_name");
      _region_cat = ArrayUtils.find(fr.vec(_r_name_idx).domain(),REGION);
      _mins = mins;
    }
    @Override public void map( Chunk[] cs, NewChunk[] ncs ) {
      Chunk partkeys= cs[_partkey_idx];
      Chunk costs   = cs[_cost_idx];
      Chunk r_names = cs[_r_name_idx];
      // The Main Hot Loop
      for( int i=0; i<costs._len; i++ )
        if( r_names.at8(i)==_region_cat ) {
          long partkey = partkeys.at8(i);
          double cost = costs.atd(i);
          Double min = _mins.get(partkey);
          if( (double)min==cost )
            SQL.copyRow(cs,ncs,i);
        }
    }
  }
  
}
