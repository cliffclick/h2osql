package org.cliffc.sql;

import water.*;
import water.fvec.*;
import water.TestUtil;
import water.parser.*;
import water.util.*;
import org.joda.time.DateTime;

import java.io.IOException;
import java.util.Arrays;

public class SQL {
  // Scale-factor; also part of the data directory name.
  public static final String SCALE_FACTOR = "sf-1";

  // The TPCH Schema
  public static Table LINEITEM = new Table("lineitem",new String[]{"id","orderkey","ps_id","linenumber","quantity","extendedprice","discount","tax","returnflag","linestatus","shipdate","commitdate","receiptdate","shipinstruct","shipmode","comment"});
  
  public static void main( String[] args ) throws IOException {

    H2O.main(new String[0]);


    long t = System.currentTimeMillis();
    System.out.println("Loading TPCH data for "+SCALE_FACTOR);

    // Always first column is the index column, and is just a number.
    // If a column name appears in another dataset, it refers via index.
    Frame customer = loadData("customer",new String[]{"custkey","name","address","nationkey","phone","acctbal","mktsegment","comment"});
    Frame lineitem = LINEITEM.frame();
    Frame nation   = loadData("nation",new String[]{"nationkey","name","regionkey","comment"});
    Frame orders   = loadData("orders",new String[]{"orderkey","custkey","orderstatus","totalprice","orderdate","orderpriority","cleark","shippriority","comment"});
    Frame part     = loadData("part",new String[]{"partkey","name","mfgr","brand","type","size","container","retailprice","comment"});
    Frame partsupp = loadData("partsupp",new String[]{"ps_id","partkey","suppkey","availqty","supplycost","comment"});
    Frame region   = loadData("region",new String[]{"regionkey","name","comment"});
    Frame supplier = loadData("supplier",new String[]{"suppkey","name","address","nationkey","phone","acctbal","comment"});
    long loaded = System.currentTimeMillis();
    System.out.println("Data loaded in "+(loaded-t)+" msec"); t=loaded;
    
    // Query#1
    for( int i=0; i<5; i++ ) {
      do_q1(lineitem);
      long t_q1 = System.currentTimeMillis();
      System.out.println("Query 1 "+(t_q1-t)+" msec"); t=t_q1;
    }
    
    System.exit(0);
  }


  static void do_q1(Frame lineitem) {
    Query1 q1 = new Query1().doAll(lineitem);
    Vec vret = lineitem.vec("returnflag");
    Vec vlst = lineitem.vec("linestatus");
    String[] rflags = vret.domain();
    String[] lnstss = vlst.domain();
    int max_ret = (int)vret.max()+1;
    int max_lst = (int)vlst.max()+1;

    for( int i=0; i<max_ret; i++ ) {
      for( int j=0; j<max_lst; j++ ) {
        if( q1.cnts[i][j]!=0 )
          System.out.printf("%s|%s|%d|%f|%f|%f|%f|%f|%f|%d\n",
                            rflags[i],
                            lnstss[j],
                            q1.sum_qty[i][j],
                            q1.sum_base_price[i][j],
                            q1.sum_disc_price[i][j],
                            q1.sum_charge[i][j],
                            (double)q1.sum_qty[i][j]/q1.cnts[i][j],
                            q1.sum_base_price[i][j]/q1.cnts[i][j],
                            q1.sum_disc[i][j]/q1.cnts[i][j],
                            q1.cnts[i][j] );
      }
    }
  }
  
  private static class Query1 extends MRTask<Query1> {
    static long last_date = new DateTime("1998-12-1").plusDays(-90).getMillis();
    
    int   [/*rflg*/][/*lsts*/] cnts;
    int   [/*rflg*/][/*lsts*/] sum_qty;
    double[/*rflg*/][/*lsts*/] sum_base_price;
    double[/*rflg*/][/*lsts*/] sum_disc_price;
    double[/*rflg*/][/*lsts*/] sum_charge;
    double[/*rflg*/][/*lsts*/] sum_disc;
    
    // Bunch of sums, grouped-by linestatus and returnflag
    @Override public void map( Chunk[] cs ) {
      Chunk discount     = cs[LINEITEM.colnum("discount")];
      Chunk extendedprice= cs[LINEITEM.colnum("extendedprice")];
      Chunk linestatus   = cs[LINEITEM.colnum("linestatus")];
      Chunk quantity     = cs[LINEITEM.colnum("quantity")];
      Chunk returnflag   = cs[LINEITEM.colnum("returnflag")];
      Chunk shipdate     = cs[LINEITEM.colnum("shipdate")];
      Chunk taxs         = cs[LINEITEM.colnum("tax")];
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
      
      for( int i=0; i<cs[0]._len; i++ ) {
        double disc =      discount     .atd(i);
        double eprc =      extendedprice.atd(i);
        int    lsts = (int)linestatus   .at8(i);
        int    qty  = (int)quantity     .at8(i);
        int    rflg = (int)returnflag   .at8(i);
        long   date =      shipdate     .at8(i);
        double tax  =      taxs         .atd(i);
        if( date >= last_date ) continue;

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
  }


  
  // Parse & load TPCH data.  Its in a CSV format without headers (supplied as
  // an argument) and uses '|' as the line separator.
  static Frame loadData(String base, String[] headers) {
    try {
      String fname = "c:/Users/cliffc/Desktop/raicode/packages/DelveBenchmarks/src/TPCH/data/"+SCALE_FACTOR+"/"+base+".tbl";
      NFSFileVec nfs = NFSFileVec.make(fname);      
      Key[] keys = new Key[]{nfs._key};
      ParseSetup guess1 = new ParseSetup(DefaultParserProviders.CSV_INFO, (byte)'|', false, -1, headers.length, null, new ParseWriter.ParseErr[0]);
      ParseSetup guess2 = ParseSetup.guessSetup(keys, guess1);
      guess2.setColumnNames(headers);
      return ParseDataset.parse(Key.make(base+".hex"), keys, true, guess2);
    } catch( IOException ioe ) {
      throw new RuntimeException(ioe);
    }
  }

  private static class Table {
    final String _name;
    final String[] _headers;
    Frame _frame;
    public Table(String name, String[] headers) {
      _name = name;
      _headers = headers;
    }
    // Delayed frame-build
    Frame frame() { return _frame==null ? (_frame=loadData(_name,_headers)) : _frame; }
    int colnum(String h) {
      for( int i=0; i<_headers.length; i++ )
        if( h.equals(_headers[i]) )
          return i;
      return -1;
    }
  }
}
