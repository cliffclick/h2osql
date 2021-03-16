package org.cliffc.sql;

import water.*;
import water.fvec.*;
import water.parser.*;
import water.util.FrameUtils;

import java.io.IOException;

public class SQL {
  // Scale-factor; also part of the data directory name.
  public static final String SCALE_FACTOR = "sf-0.01";

  // The TPCH Schema
  // Always first column is the index column, and is just a number.
  // If a column name appears in another dataset, it refers via index.
  public static Table CUSTOMER = new Table("customer",new String[]{"custkey","name","address","nationkey","phone","acctbal","mktsegment","comment"},new String[]{"address","comment"});
  public static Table LINEITEM = new Table("lineitem",new String[]{"id","orderkey","ps_id","linenumber","quantity","extendedprice","discount","tax","returnflag","linestatus","shipdate","commitdate","receiptdate","shipinstruct","shipmode","comment"},new String[]{"comment"});
  public static Table NATION   = new Table("nation",new String[]{"nationkey","name","regionkey","comment"},new String[]{"comment"});
  public static Table ORDERS   = new Table("orders",new String[]{"orderkey","custkey","orderstatus","totalprice","orderdate","orderpriority","cleark","shippriority","comment"},new String[]{"comment"});
  public static Table PART     = new Table("part",new String[]{"partkey","name","mfgr","brand","type","size","container","retailprice","comment"},new String[]{"comment"});
  public static Table PARTSUPP = new Table("partsupp",new String[]{"ps_id","partkey","suppkey","availqty","supplycost","comment"},new String[]{"comment"});
  public static Table REGION   = new Table("region",new String[]{"regionkey","name","comment"},new String[]{"comment"});
  public static Table SUPPLIER = new Table("supplier",new String[]{"suppkey","name","address","nationkey","phone","acctbal","comment"},new String[]{"address","comment"});


  
  public static void main( String[] args ) throws IOException {
    H2O.main(new String[0]);
    
    // Load all the tables
    long t = System.currentTimeMillis();
    System.out.println("Loading TPCH data for "+SCALE_FACTOR);
    CUSTOMER.frame();
    LINEITEM.frame();
    NATION  .frame();
    ORDERS  .frame();
    PART    .frame();
    PARTSUPP.frame();
    REGION  .frame();
    SUPPLIER.frame();
    long loaded = System.currentTimeMillis();
    System.out.println("Data loaded in "+(loaded-t)+" msec"); t=loaded;
    
    // Query#1
    Frame q1 = Query1.run();
    System.out.println(q1.toTwoDimTable());
    q1.delete();
    long t_q1 = System.currentTimeMillis();
    System.out.print("Query 1 "+(t_q1-t)+" msec, "); t=t_q1;
    for( int i=0; i<5; i++ ) {
      Query1.run().delete();
      t_q1 = System.currentTimeMillis();
      System.out.print(""+(t_q1-t)+" msec, "); t=t_q1;
    }
    System.out.println();

    // Query#2
    Frame q2 = Query2.run();
    System.out.println(q2.toTwoDimTable());
    q2.delete();
    long t_q2 = System.currentTimeMillis();
    System.out.print("Query 2 "+(t_q2-t)+" msec, "); t=t_q2;
    
    System.exit(0);
  }

  // Wrapper around a TPCH table
  public static class Table {
    final String _name;         // Base file name, table name
    final String[] _headers;                 // Schema is not in the data, passed in on construction
    private final String[] _skipped_columns; // Do not load all columns
    Frame _frame;                            // Loaded data
    public Table(String name, String[] headers, String[] skipped_columns) {
      _name = name;
      _headers = headers;
      _skipped_columns = skipped_columns;
    }
    // Delayed frame-build so H2O can start.
    Frame frame() { return _frame==null ? init(_frame=loadData()) : _frame; }

    // Parse & load TPCH data.  Its in a CSV format without headers (supplied as
    // an argument) and uses '|' as the line separator.
    private Frame loadData() {
      try {
        String fname = "c:/Users/cliffc/Desktop/raicode/packages/DelveBenchmarks/src/TPCH/data/"+SCALE_FACTOR+"/"+_name+".tbl";
        NFSFileVec nfs = NFSFileVec.make(fname);      
        Key[] keys = new Key[]{nfs._key};

        // Force CSV parse, with '|' field separator, no-single-quotes,
        // no-header, give column count based on schema, no heuristic data
        // (yet), no errors (yet).
        ParseSetup guess1 = new ParseSetup(DefaultParserProviders.CSV_INFO, (byte)'|', false, -1, _headers.length, null, new ParseWriter.ParseErr[0]);
        // Set column headers (but still none in the data)
        guess1.setColumnNames(_headers);
        // Final setup after looking at the data
        ParseSetup guess2 = ParseSetup.guessSetup(keys, guess1);
        // Skip loading some fields
        if( _skipped_columns!=null ) {
          int[] nx = new int[_skipped_columns.length];
          for( int i=0; i<nx.length; i++ ) nx[i] = colnum(_skipped_columns[i]);
          guess2.setSkippedColumns(nx);
        }
        // Parse a frame and return it
        return ParseDataset.parse(Key.make(_name+".hex"), keys, true, guess2);
      } catch( IOException ioe ) {
        throw new RuntimeException(ioe);
      }
    }

    // Any generic TPCH cleanup
    Frame init(Frame fr) {
      System.out.println(fr);
      System.out.println(FrameUtils.chunkSummary(fr));
      System.out.println(fr.toTwoDimTable());
      
      return fr;
    }

    int colnum(String h) {
      for( int i=0; i<_headers.length; i++ )
        if( h.equals(_headers[i]) )
          return i;
      return -1;
    }
  }
  
  // Make a new small-vector key, suitable for small Frame/Vec returns.
  public static Key<Vec> vkey() { return Vec.VectorGroup.VG_LEN1.addVec(); }

  // Pretty sure this exists in H2O, just missing it
  public static void copyRow(Chunk[] cs, NewChunk[] ncs, int row) {
    BufferedString bStr = new BufferedString();
    for( int i=0; i<cs.length; i++ ) {
      if( cs[i].isNA(row)) ncs[i].addNA();
      else if( cs[i] instanceof CStrChunk ) ncs[i].addStr(cs[i].atStr(bStr,row));
      else ncs[i].addNum(cs[i].atd(row));
    }
  }
  
}
