package org.cliffc.sql;

import water.*;
import water.fvec.*;
import water.nbhm.NonBlockingSetInt;
import water.rapids.Merge;
import water.util.ArrayUtils;
import java.util.Arrays;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

/**

The Volume Shipping Query finds, for two given nations, the gross discounted
revenues derived from lineitems in which parts were shipped from a supplier in
either nation to a customer in the other nation during 1995 and 1996.  The query
lists the supplier nation, the customer nation, the year, and the revenue from
shipments that took place in that year. The query orders the answer by Supplier
nation, Customer nation, and year (all ascending).

NATION1 is randomly selected within the list of values defined for N_NAME in Clause 4.2.3;

NATION2 is randomly selected within the list of values defined for N_NAME in
Clause 4.2.3 and must be different from the value selected for NATION1 in item
1 above.

select
    supp_nation,
    cust_nation,
    l_year,
    sum(volume) as revenue
from
    (
        select
            n1.n_name as supp_nation,
            n2.n_name as cust_nation,
            extract(year from l_shipdate) as l_year,
            l_extendedprice * (1 - l_discount) as volume
        from
            supplier,
            lineitem,
            orders,
            customer,
            nation n1,
            nation n2
        where
            s_suppkey = l_suppkey
            and o_orderkey = l_orderkey
            and c_custkey = o_custkey
            and s_nationkey = n1.n_nationkey
            and c_nationkey = n2.n_nationkey
            and (
                (n1.n_name = '@@1' and n2.n_name = '@@2') or
                (n1.n_name = '@@2' and n2.n_name = '@@1')
            )
            and l_shipdate between date '1995-01-01' and date '1996-12-31'
    ) as shipping
group by
    supp_nation,
    cust_nation,
    l_year
order by
    supp_nation,
    cust_nation,
    l_year;

SF-0.01 Validation
FRANCE                   |GERMANY                  |1995|268068.5774
FRANCE                   |GERMANY                  |1996|303862.298
GERMANY                  |FRANCE                   |1995|621159.4882
GERMANY                  |FRANCE                   |1996|379095.8854

*/

public class Query7 implements SQL.Query {
  @Override public String name() { return "Query7"; }
  static final boolean PRINT_TIMING = false;

  static final int YEAR=1995;
  static final long LOW_DATE  = new DateTime(""+YEAR+"-01-01",DateTimeZone.UTC).getMillis();
  static final long MID_DATE  = new DateTime("1996-01-01",DateTimeZone.UTC).getMillis();
  static final long HIGH_DATE = new DateTime("1996-12-31",DateTimeZone.UTC).getMillis();

  static final String NATION1 = "FRANCE";
  static final String NATION2 = "GERMANY";

  // Query plan:

  // per-lineitem, get year & vol; l_orderkey => o_orderkey/o_custkey => c_custkey => cnation
  //                               l_suppkey => s_suppkey;
  //                               test (cnation & snation are each N1/N2 in some ordre)
  // Filter customers by N1, make BitSet.
  // Filter orders by customers in N1; bitset of orderkeys
  // Filter suppliers by N2, make BitSet.
  // Filter LINEITEM by
  // - shipdate
  // - a customer from N1
  // - a supplier from N2
  // - GroupBy: supp x cust x year (2x2x2 matrix)
  
  
  @Override public Frame run() {
    // Find factor values for NATION1 and NATION2
    Frame nation = SQL.NATION.frame();
    Vec nation_name = nation.vec("n_name");
    String[] n_domain = nation_name.domain();
    final int N1_FACT = ArrayUtils.find(n_domain,NATION1);
    final int N2_FACT = ArrayUtils.find(n_domain,NATION2);

    // Filter customers by N1/N2, making a BitSet of custkeys
    Frame customer = SQL.CUSTOMER.frame();
    assert customer.vec("n_name").domain()==nation_name.domain();
    FilterColSet fcsc = new FilterColSet(N1_FACT,N2_FACT).doAll(customer.vec("n_name"),customer.vec("custkey"));
    NonBlockingSetInt csn1 = fcsc._n1s, csn2 = fcsc._n2s;

    // Filter orders by customers in N1/N2, making a BitSet of orderkeys
    Frame ords0 = SQL.ORDERS.frame();
    Frame ords1 = ords0.subframe(new String[]{"custkey","orderkey"});
    FilterColSet2 fcs2 = new FilterColSet2(csn1,csn2).doAll(ords1);
    NonBlockingSetInt osn1 = fcs2._osn1, osn2 = fcs2._osn2;
    
    // Filter suppliers by N1/N2, making a BitSet of suppkeys
    Frame supplier = SQL.SUPPLIER.frame();
    FilterColSet fcss = new FilterColSet(N1_FACT,N2_FACT).doAll(supplier.vec("n_name"),supplier.vec("suppkey"));
    NonBlockingSetInt ssn1 = fcss._n1s, ssn2 = fcss._n2s;

    // Filter LINEITEM by shipdate, an order from N1 & a supplier from N2 (or
    // vice-versa).  GroupBy volume by supp_nation x order_nation x year (a 2x2x2 matrix).
    Frame line0 = SQL.LINEITEM.frame();
    Frame line1 = line0.subframe(new String[]{"shipdate","orderkey","suppkey","extendedprice","discount"});
    FilterGroup fg = new FilterGroup(osn1,osn2,ssn1,ssn2).doAll(line1);

    // Trim out missing counts; count rows to keep
    int nrows=0;
    for( int i=0; i<fg._osy.length; i++ )
      for( int j=0; j<fg._osy[0].length; j++ )
        for( int k=0; k<fg._osy[0][0].length; k++ )
          if( fg._osy[i][j][k]!=0 ) nrows++;
    
    // Format as a frame
    Frame fr = new Frame();
    fr.add("supp_nation",fg.vec(nrows,((ox,sx,yx) -> sx), new String[]{NATION1,NATION2}));
    fr.add("cust_nation",fg.vec(nrows,((ox,sx,yx) -> ox), new String[]{NATION1,NATION2}));
    fr.add("l_year"     ,fg.vec(nrows,((ox,sx,yx) -> yx+YEAR), null));
    fr.add("revenue"    ,fg.vec(nrows,((ox,sx,yx) -> fg._osy[ox][sx][yx]), null));

    // Sort
    Frame rez = Merge.sort(fr,fr.find(new String[]{"supp_nation","cust_nation","l_year"}));
    fr.delete();
    return rez;
  }

  // Filter customers/suppliers by {NATION1,NATION2} and return a BitSet of
  // custkey/suppkey for both.
  private static class FilterColSet extends MRTask<FilterColSet> {
    final int _n1, _n2;
    transient NonBlockingSetInt _n1s, _n2s;
    @Override protected void setupLocal() { _n1s = new NonBlockingSetInt(); _n2s = new NonBlockingSetInt(); }
    FilterColSet(int n1, int n2) {_n1=n1; _n2=n2; }
    @Override public void map( Chunk cn, Chunk ckey ) {
      for( int i=0; i<cn._len; i++ ) {
        if( cn.at8(i)==_n1 ) _n1s.add((int)ckey.at8(i));
        if( cn.at8(i)==_n2 ) _n2s.add((int)ckey.at8(i));
      }
    }
    @Override public void reduce( FilterColSet fcs ) {
      if( _n1s != fcs._n1s || _n2s != fcs._n2s )
        throw new RuntimeException("Distribution not implemented");
    }
  }

  // Filter orders by sets of customers, and return BitSets of orderkeys.
  private static class FilterColSet2 extends MRTask<FilterColSet2> {
    final     NonBlockingSetInt _csn1, _csn2;
    transient NonBlockingSetInt _osn1, _osn2;
    @Override protected void setupLocal() { _osn1 = new NonBlockingSetInt(); _osn2 = new NonBlockingSetInt(); }
    FilterColSet2( NonBlockingSetInt csn1, NonBlockingSetInt csn2 ) { _csn1=csn1; _csn2=csn2; }
    @Override public void map( Chunk custkey, Chunk orderkey ) {
      for( int i=0; i<custkey._len; i++ ) {
        if( _csn1.contains((int)custkey.at8(i)) ) _osn1.add((int)orderkey.at8(i));
        if( _csn2.contains((int)custkey.at8(i)) ) _osn2.add((int)orderkey.at8(i));
      }
    }
    @Override public void reduce( FilterColSet2 fcs2 ) {
      if( _osn1 != fcs2._osn1 || _osn2 != fcs2._osn2 )
        throw new RuntimeException("Distribution not implemented");
    }
  }    

  // Filter LINEITEM by shipdate, an order from N1 & a supplier from N2 (or
  // vice-versa).  GroupBy volume by supp_nation x order_nation x year (a 2x2x2 matrix).
  private static class FilterGroup extends MRTask<FilterGroup> {
    final NonBlockingSetInt _osn1, _osn2; // Orders    in N1 or N2
    final NonBlockingSetInt _ssn1, _ssn2; // Suppliers in N1 or N2
    double[][][] _osy;
    FilterGroup( NonBlockingSetInt osn1, NonBlockingSetInt osn2,
                 NonBlockingSetInt ssn1, NonBlockingSetInt ssn2 ) {
      _osn1=osn1; _osn2=osn2;
      _ssn1=ssn1; _ssn2=ssn2;
    }
    @Override public void map( Chunk cs[] ) {
      Chunk shipdates = cs[0];
      Chunk orderkeys = cs[1];
      Chunk suppkeys  = cs[2];
      Chunk exprices  = cs[3];
      Chunk discounts = cs[4];
      double[][][] osy = new double[2][2][2];
      for( int i=0; i<suppkeys._len; i++ ) {
        long shipdate = shipdates.at8(i);
        if( LOW_DATE <= shipdate && shipdate <= HIGH_DATE ) {
          int yr = shipdate < MID_DATE ? 0 : 1;
          double expr   = exprices .atd(i);
          double disc   = discounts.atd(i);
          int orderkey = (int)orderkeys.at8(i);
          int suppkey  = (int)suppkeys .at8(i);
          if( _osn1.contains(orderkey) && _ssn2.contains(suppkey) )
            osy[0][1][yr] += expr*(1.0-disc);
          if( _osn2.contains(orderkey) && _ssn1.contains(suppkey) )
            osy[1][0][yr] += expr*(1.0-disc);
        }
        _osy = osy;
      }
    }
    @Override public void reduce( FilterGroup fg ) {
      ArrayUtils.add(_osy,fg._osy);
    }
    
    // Helper to print final result Frame
    interface Fcn_IIID { abstract double run(int i, int j, int k); }
    Vec vec(int nrow, Fcn_IIID fcn) { return vec(nrow,fcn,null); }
    Vec vec(int nrow, Fcn_IIID fcn, String[] domain) {
      double[] ds = new double[nrow];
      int row=0;
      for( int i=0; i<_osy.length; i++ )
        for( int j=0; j<_osy[0].length; j++ )
          for( int k=0; k<_osy[0][0].length; k++ )
            if( _osy[i][j][k]!=0 )
              ds[row++] = fcn.run(i,j,k);
      return Vec.makeVec(ds,domain,SQL.vkey());
    }

  }    
  
}
