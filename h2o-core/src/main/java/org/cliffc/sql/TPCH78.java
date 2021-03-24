package org.cliffc.sql;

import water.*;
import water.fvec.*;
import water.nbhm.NonBlockingSetInt;
import water.util.ArrayUtils;
import java.util.BitSet;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

// Query8 is NOT FUNCTIONAL.  Stopped mid-development, since Q1-Q7 prove the
// speed point of H2O on traditional SQL queires.


/**

The market share for a given nation within a given region is defined as the
fraction of the revenue, the sum of [l_extendedprice * (1-l_discount)], from the
products of a specified type in that region that was supplied by suppliers
from the given nation.  The query determines this for the years 1995 and 1996
presented in this order.

select
    o_year,
    sum(case
        when nation = '@@1' then volume
        else 0
    end) / sum(volume) as mkt_share
from
    (
        select
            extract(year from o_orderdate) as o_year,
            l_extendedprice * (1 - l_discount) as volume,
            n2.n_name as nation
        from
            part,
            supplier,
            lineitem,
            orders,
            customer,
            nation n1,
            nation n2,
            region
        where
            p_partkey = l_partkey
            and s_suppkey = l_suppkey
            and l_orderkey = o_orderkey
            and o_custkey = c_custkey
            and c_nationkey = n1.n_nationkey
            and n1.n_regionkey = r_regionkey
            and r_name = '@@2'
            and s_nationkey = n2.n_nationkey
            and o_orderdate between date '1995-01-01' and date '1996-12-31'
            and p_type = '@@3'
    ) as all_nations
group by
    o_year
order by
    o_year;

SF-0.01 Validation
1995|0
1996|0

*/

public class Query8 implements SQL.Query {
  @Override public String name() { return "Query8"; }
  static final boolean PRINT_TIMING = false;

  //static final String NATION = "BRAZIL";
  //static final String REGION = "AMERICA";
  static final String NATION = "GERMANY";
  static final String REGION = "EUROPE";
  static final String TYPE = "ECONOMY ANODIZED STEEL";

  static final long LOW_DATE  = new DateTime("1995-01-01",DateTimeZone.UTC).getMillis();
  static final long HIGH_DATE = new DateTime("1996-12-31",DateTimeZone.UTC).getMillis();
  
  // Query plan:

  // Filter orders by customers in region & date
  // - split into 1995 & 1996
  // Filter parts by type
  // Filter suppliers by nation
  // Filter lineitems parts & suppliers;
  // JOIN lineitems with orders
  // - groupby order-year & suppler-nation; sum volume
  // Then (small-data)
  // - sum nation-volume (per-year), compute fraction.
  
  @Override public Frame run() {
    // Find factor values
    Frame nation = SQL.NATION.frame();
    final int NFACT = ArrayUtils.find(nation.vec("n_name").domain(),NATION);
    Frame region = SQL.REGION.frame();
    final int RFACT = ArrayUtils.find(region.vec("r_name").domain(),REGION);
    
    BitSet natRegion = new BitSet();    
    Vec.Reader vnregion= nation.vec("r_name").new Reader();
    for( int i=0; i<vnregion.length(); i++ )
      if( vnregion.at8(i)==RFACT )
        natRegion.set(i);

    // Filter customers by REGION, making a BitSet of custkeys in that region.
    Frame customer = SQL.CUSTOMER.frame();
    assert customer.vec("n_name").domain()==nation.vec("n_name").domain();
    NonBlockingSetInt custRegion = new FilterCust(natRegion).doAll(customer.vec("n_name"),customer.vec("custkey"))._nbsi;
    System.out.println("Customers in "+REGION+" "+custRegion);

    // Filter ORDERS by CUSTOMERS in REGION, making a BitSet of orderkeys within Region & date
    Frame ords0 = SQL.ORDERS.frame();
    Frame ords1 = ords0.subframe(new String[]{"custkey","orderkey","orderdate"});
    NonBlockingSetInt ordersRegion = new FilterOrders(custRegion).doAll(ords1)._nbsi;
    System.out.println("Orders in "+REGION+" "+ordersRegion.size());

    // Filter PART by TYPE, making a BitSet of partKeys
    Frame parts0 = SQL.PART.frame();
    Frame parts1 = parts0.subframe(new String[]{"type","partkey"});
    final int TFACT = ArrayUtils.find(parts1.vec("type").domain(),TYPE);
    NonBlockingSetInt partsType = new FilterParts(TFACT).doAll(parts1)._nbsi;
    System.out.println("Parts with type "+TYPE+" "+partsType);

    // Filter SUPPLIERS by NATION
    Frame supps0 = SQL.SUPPLIER.frame();
    Frame supps1 = supps0.subframe(new String[]{"suppkey","n_name"});
    Frame supps2 = new SQL.FilterCol(supps1.find("n_name"),NFACT).doAll(supps1.types(),supps1).outputFrame(supps1.names(),supps1.domains());
    System.out.println("Suppliers with nation "+NATION+" "+supps2.toTwoDimTable(0,10,true));

    // Filter LINEITEMS by matching partkey & suppkey
    
    return null;
  }

  private static class FilterCust extends MRTask<FilterCust> {
    final BitSet _natRegion;
    transient NonBlockingSetInt _nbsi;
    @Override protected void setupLocal() { _nbsi = new NonBlockingSetInt(); }
    FilterCust( BitSet natRegion ) { _natRegion = natRegion; }
    @Override public void map( Chunk n_name, Chunk custkey ) {
      for( int i=0; i<custkey._len; i++ )
        if( _natRegion.get((int)n_name.at8(i)) )
          _nbsi.add((int)custkey.at8(i));
    }
    @Override public void reduce( FilterCust fc ) {
      if( _nbsi != fc._nbsi )
        throw new RuntimeException("Distribution not implemented");
    }    
  }
  
  private static class FilterOrders extends MRTask<FilterOrders> {
    final NonBlockingSetInt _custRegion;
    transient NonBlockingSetInt _nbsi;
    @Override protected void setupLocal() { _nbsi = new NonBlockingSetInt(); }
    FilterOrders( NonBlockingSetInt custRegion ) { _custRegion = custRegion; }
    @Override public void map( Chunk custkey, Chunk orderkey, Chunk orderdate ) {
      for( int i=0; i<custkey._len; i++ ) {
        long date = orderdate.at8(i);
        if( _custRegion.contains((int)custkey.at8(i)) &&
            LOW_DATE <= date && date <= HIGH_DATE )
          _nbsi.add((int)orderkey.at8(i));
      }
    }
    @Override public void reduce( FilterOrders fo ) {
      if( _nbsi != fo._nbsi )
        throw new RuntimeException("Distribution not implemented");
    }    
  }

  private static class FilterParts extends MRTask<FilterParts> {
    final int _type;
    transient NonBlockingSetInt _nbsi;
    @Override protected void setupLocal() { _nbsi = new NonBlockingSetInt(); }
    FilterParts( int type ) { _type = type; }
    @Override public void map( Chunk types, Chunk partkeys ) {
      for( int i=0; i<types._len; i++ )
        if( types.at8(i)==_type )
          _nbsi.add((int)partkeys.at8(i));
    }
    @Override public void reduce( FilterParts fp ) {
      if( _nbsi != fp._nbsi )
        throw new RuntimeException("Distribution not implemented");
    }    
  }  
  
}
