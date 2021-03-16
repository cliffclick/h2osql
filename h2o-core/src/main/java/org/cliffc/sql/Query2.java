package org.cliffc.sql;

import water.*;
import water.fvec.*;
import water.util.ArrayUtils;
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

  static boolean[] IS_BRASS;

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

    Frame part     = SQL.PART    .frame();
    Frame supplier = SQL.SUPPLIER.frame();

    // Compute the categorical constant for the TYPE
    String[] types = part.vec("type").domain();
    IS_BRASS = new boolean[types.length];
    for( int i=0; i<types.length; i++ )
      IS_BRASS[i] = types[i].contains(TYPE);

    // Filter parts
    Frame xpart = new FilterPart().doAll(part.types(),part).outputFrame(part.names(),part.domains());
    // Repack the (very) sparse result into fewer chunks
    Key<Frame> kpart0 = Key.make("part0.hex");
    int nchunks = (int)((xpart.numRows()+1023)/1024);
    H2O.submitTask(new RebalanceDataSet(xpart, kpart0,nchunks)).join();
    xpart.delete();
    Frame part0 = kpart0.get();
    System.out.println(part0);



    
    System.exit(-1);
    throw new RuntimeException("impl");
  }

  private static class FilterPart extends MRTask<FilterPart> {
    @Override public void map( Chunk[] cs, NewChunk[] ncs ) {
      Chunk type = cs[SQL.PART.colnum("type")];
      Chunk size = cs[SQL.PART.colnum("size")];
      // The Main Hot Loop
      for( int i=0; i<type._len; i++ )
        if( IS_BRASS[(int)type.at8(i)] && size.at8(i)==SIZE )
          SQL.copyRow(cs,ncs,i);
    }
  }
  
}
