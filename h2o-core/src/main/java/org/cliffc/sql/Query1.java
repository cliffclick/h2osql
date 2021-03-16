package org.cliffc.sql;

import water.*;
import water.fvec.*;
import org.joda.time.DateTime;

public class Query1 extends MRTask<Query1> {

  public static Frame do_q1() {
    // Compute Big Data rollups
    Frame lineitem = SQL.LINEITEM.frame();
    Query1 q1 = new Query1().doAll(lineitem);
  
    // Trim out missing counts; count rows to keep
    int nrows=0;
    for( int i=0; i<q1.cnts.length; i++ )
      for( int j=0; j<q1.cnts[0].length; j++ )
        if( q1.cnts[i][j]!=0 ) nrows++;
  
    // Resuling Frame is Small data, filled into local arrays.
    Frame fr = new Frame();
  
    fr.add("returnflag"    ,q1.vec(nrows,((i,j) -> i),lineitem.vec("returnflag").domain()));
    fr.add("linestatus"    ,q1.vec(nrows,((i,j) -> j),lineitem.vec("linestatus").domain()));
    fr.add("sum_qty"       ,q1.vec(nrows,((i,j) -> q1.sum_qty[i][j])));
    fr.add("sum_base_price",q1.vec(nrows,((i,j) -> q1.sum_base_price[i][j])));
    fr.add("sum_disc_price",q1.vec(nrows,((i,j) -> q1.sum_disc_price[i][j])));
    fr.add("sum_charge"    ,q1.vec(nrows,((i,j) -> q1.sum_charge[i][j])));
    fr.add("avg_qty"       ,q1.vec(nrows,((i,j) -> (double)q1.sum_qty[i][j]/q1.cnts[i][j])));
    fr.add("avg_price"     ,q1.vec(nrows,((i,j) -> q1.sum_base_price[i][j]/q1.cnts[i][j])));
    fr.add("avg_disc"      ,q1.vec(nrows,((i,j) -> q1.sum_disc[i][j]/q1.cnts[i][j])));
    fr.add("count_order"   ,q1.vec(nrows,((i,j) -> q1.cnts[i][j])));
    return fr;    
  }
  
  static long last_date = new DateTime("1998-12-1").plusDays(-90).getMillis();
  
  int   [/*rflg*/][/*lsts*/] cnts;
  int   [/*rflg*/][/*lsts*/] sum_qty;
  double[/*rflg*/][/*lsts*/] sum_base_price;
  double[/*rflg*/][/*lsts*/] sum_disc_price;
  double[/*rflg*/][/*lsts*/] sum_charge;
  double[/*rflg*/][/*lsts*/] sum_disc;
  
  // Bunch of sums, grouped-by linestatus and returnflag
  @Override public void map( Chunk[] cs ) {
    Chunk discount     = cs[SQL.LINEITEM.colnum("discount")];
    Chunk extendedprice= cs[SQL.LINEITEM.colnum("extendedprice")];
    Chunk linestatus   = cs[SQL.LINEITEM.colnum("linestatus")];
    Chunk quantity     = cs[SQL.LINEITEM.colnum("quantity")];
    Chunk returnflag   = cs[SQL.LINEITEM.colnum("returnflag")];
    Chunk shipdate     = cs[SQL.LINEITEM.colnum("shipdate")];
    Chunk taxs         = cs[SQL.LINEITEM.colnum("tax")];
    // Size of the group-bys
    int max_ret = (int)returnflag.vec().max()+1;
    int max_lst = (int)linestatus.vec().max()+1;
    // Arrays to hold the sums
    cnts           = new int   [max_ret][max_lst];
    sum_qty        = new int   [max_ret][max_lst];
    sum_base_price = new double[max_ret][max_lst];
    sum_disc_price = new double[max_ret][max_lst];
    sum_charge     = new double[max_ret][max_lst];
    sum_disc       = new double[max_ret][max_lst];

    // The Main Hot Loop
    for( int i=0; i<cs[0]._len; i++ ) {
      long   date =      shipdate     .at8(i);
      if( date >= last_date ) continue; // Filter by date
      double disc =      discount     .atd(i);
      double eprc =      extendedprice.atd(i);
      int    lsts = (int)linestatus   .at8(i);
      int    qty  = (int)quantity     .at8(i);
      int    rflg = (int)returnflag   .at8(i);
      double tax  =      taxs         .atd(i);

      cnts          [rflg][lsts] += 1;
      sum_qty       [rflg][lsts] += qty;
      sum_base_price[rflg][lsts] += eprc;
      sum_disc_price[rflg][lsts] += eprc*(1-disc);
      sum_charge    [rflg][lsts] += eprc*(1-disc)*(1+tax);
      sum_disc      [rflg][lsts] += disc;
    }
  }
  // ADD together all results
  @Override public void reduce( Query1 q ) {
    water.util.ArrayUtils.add(cnts          ,q.cnts          );
    water.util.ArrayUtils.add(sum_qty       ,q.sum_qty       );
    water.util.ArrayUtils.add(sum_base_price,q.sum_base_price);
    water.util.ArrayUtils.add(sum_disc_price,q.sum_disc_price);
    water.util.ArrayUtils.add(sum_charge    ,q.sum_charge    );
    water.util.ArrayUtils.add(sum_disc      ,q.sum_disc      );
  }

  // Helper to print final result Frame
  Vec vec(int nrow, Fcn_I_I_D fcn) { return vec(nrow,fcn,null); }
  Vec vec(int nrow, Fcn_I_I_D fcn, String[] domain) {
    double[] ds = new double[nrow];
    int row=0;
    for( int i=0; i<cnts.length; i++ )
      for( int j=0; j<cnts[0].length; j++ )
        if( cnts[i][j]!=0 )
          ds[row++] = fcn.run(i,j);
    return Vec.makeVec(ds,domain,SQL.vkey());
  }
}

interface Fcn_I_I_D { abstract double run(int i, int j); }
