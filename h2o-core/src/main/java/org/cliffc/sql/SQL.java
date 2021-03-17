package org.cliffc.sql;

import water.*;
import water.fvec.*;
import water.parser.*;
import water.rapids.Env;
import water.rapids.Session;
import water.rapids.ast.*;
import water.rapids.ast.params.*;
import water.rapids.ast.prims.mungers.AstMerge;
import water.rapids.vals.ValFrame;
import water.util.FrameUtils;
import water.util.SB;

import java.io.IOException;

public class SQL {
  // Scale-factor; also part of the data directory name.
  public static final String SCALE_FACTOR = "sf-1";

  // The TPCH Schema
  // Always first column is the index column, and is just a number.
  // If a column name appears in another dataset, it refers via index.
  public static final Table CUSTOMER = new Table("customer",new String[]{"custkey","name","address","nationkey","phone","acctbal","mktsegment","comment"},new String[]{"address","comment"});
  public static final Table LINEITEM = new Table("lineitem",new String[]{"id","orderkey","ps_id","linenumber","quantity","extendedprice","discount","tax","returnflag","linestatus","shipdate","commitdate","receiptdate","shipinstruct","shipmode","comment"},new String[]{"comment"});
  public static final Table NATION   = new Table("nation",new String[]{"nationkey","n_name","regionkey","n_comment"},new String[]{"n_comment"});
  public static final Table ORDERS   = new Table("orders",new String[]{"orderkey","custkey","orderstatus","totalprice","orderdate","orderpriority","clerk","shippriority","comment"},new String[]{"comment"});
  public static final Table PART     = new Table("part",new String[]{"partkey","p_name","mfgr","brand","type","size","container","retailprice","p_comment"},new String[]{"p_comment"});
  public static final Table PARTSUPP = new Table("partsupp",new String[]{"partkey","suppkey","availqty","supplycost","ps_comment"},new String[]{"ps_comment"});
  public static final Table REGION   = new Table("region",new String[]{"regionkey","r_name","r_comment"},new String[]{"r_comment"});
  public static final Table SUPPLIER = new Table("supplier",new String[]{"suppkey","s_name","s_address","nationkey","phone","acctbal","s_comment"},null);

  public static Frame NATION_REGION;          // All JOINed
  public static Frame NATION_REGION_SUPPLIER; // All JOINed
  public static Frame NATION_REGION_SUPPLIER_PARTSUPP; // All JOINed
  
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
    
    REGION  .frame().toCategoricalCol(REGION  .frame().find("r_name"));
    NATION  .frame().toCategoricalCol(NATION  .frame().find("n_name"));
    SUPPLIER.frame().toCategoricalCol(SUPPLIER.frame().find("s_name"));
    
    long loaded = System.currentTimeMillis();
    System.out.println("Data loaded in "+(loaded-t)+" msec"); t=loaded;

    // Run a few common JOINs.
    NATION_REGION = join(NATION.frame(),REGION.frame());
    NATION_REGION_SUPPLIER = join(NATION_REGION,SUPPLIER.frame());
    NATION_REGION_SUPPLIER_PARTSUPP = join(NATION_REGION_SUPPLIER,PARTSUPP.frame());
    System.out.println(NATION_REGION_SUPPLIER_PARTSUPP); // Print size of final join
    
    long t_join = System.currentTimeMillis();
    System.out.println("JOINs done in "+(t_join-t)+" msec"); t=t_join;
    System.out.println();

    // Run all queries once
    //Query[] querys = new Query[]{new Query1(),new Query2(),new Query3()};
    Query[] querys = new Query[]{new Query3()}; // DEBUG one query
    System.out.println("--- Run Once ---");
    for( Query query : querys ) {
      Frame q = query.run();
      System.out.println(q.toTwoDimTable());
      q.delete();
      long t_q = System.currentTimeMillis();
      System.out.println(query.name()+" "+(t_q-t)+" msec "); t=t_q;
    }

    System.out.println("--- Run Many ---");
    for( Query query : querys ) {
      System.out.print(query.name()+" ");
      for( int i=0; i<5; i++ ) {
        query.run().delete();
        long t_q = System.currentTimeMillis();
        System.out.print(""+(t_q-t)+" msec, "); t=t_q;
      }
      System.out.println();
    }                   
    System.out.println();
    
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
      //System.out.println(FrameUtils.chunkSummary(fr));
      //System.out.println(fr.toTwoDimTable());      
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

  // Wrapper for JOIN.  Columns with matching names become the join key.
  // Does not delete either Frame.
  public static Frame join( Frame lhs, Frame rhs ) {
    // Wrap the Rapids.ASTMerge code.
    AstRoot ast_lhs = new AstFrame(lhs);
    AstRoot ast_rhs = new AstFrame(rhs);
    AstRoot ast_all_left = new AstNum(0); // boolean, exclude LHS if no match in RHS
    AstRoot ast_all_rite = new AstNum(0); // boolean, exclude RHS if no match in LHS
    AstRoot ast_by_left = new AstNumList(); // Auto-pick matching columns
    AstRoot ast_by_rite = new AstNumList(); // Auto-pick matching columns
    AstRoot ast_method = new AstStr("auto"); // Auto-pick method

    Env env = new Env(new Session());
    Env.StackHelp stk = env.stk();

    Frame fr = new AstMerge().apply(env,stk,new AstRoot[]{null,ast_lhs,ast_rhs,ast_all_left,ast_all_rite,ast_by_left,ast_by_rite,ast_method}).getFrame();
    //System.out.println(fr);
    //System.out.println(fr.toTwoDimTable());
    return fr;
  }

  // Repack a sparse frame.  Deletes old frame & returns a new one with the same key
  public static Frame compact( Frame fr ) {
    if( fr.anyVec().nChunks()==1 ) return fr; // No change
    
    Key<Frame> old = fr.getKey();
    // Repack the (very) sparse result into fewer chunks
    Key<Frame> key = Key.make("tmp_compact");
    int nchunks = (int)((fr.numRows()+1023)/1024);
    H2O.submitTask(new RebalanceDataSet(fr, key, nchunks)).join();
    fr.delete();
    Frame rez = key.get();
    if( old != null ) DKV.put(old,rez);
    DKV.put(key,null);
    return rez;
  }

  static String histo( Frame fr, String name ) {
    Vec vec = fr.vec(name);
    double base  = vec.base  ();
    double stride= vec.stride();
    long[] bins  = vec.bins  ();

    SB sb = new SB("--- ").p(name).p(" ---").nl();
    for( int i=0; i<bins.length; i++ )
      if( bins[i]!=0 )
        sb.p(i*stride+base).p(':').p(bins[i]).nl();
    return sb.toString();
  }
  
  public interface Query { abstract Frame run(); abstract String name(); }
}
