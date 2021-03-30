package org.cliffc.sql;

import water.*;
import water.fvec.*;
import water.nbhm.NonBlockingHashMapLong;
import water.parser.*;
import water.util.SB;
import water.util.PrettyPrint;

import java.util.Arrays;
import java.util.ArrayList;
import java.io.IOException;
import java.io.File;

// TSMB Queries from Rel.
//  raicode/bench/TSMB/queries.rel 


public class TSMB {
  // Scale-factor; also part of the data directory name.
  public static final String SCALE_FACTOR = "sf1";
  public static final String DIRNAME = "c:/Users/cliffc/Desktop/TSMB_DATA/social-network-"+SCALE_FACTOR+"-merged-fk/";

  // The TSMB Data
  public static Frame CITY;
  public static Frame CITY_ISPARTOF_COUNTRY;
  public static Frame COMMENT;
  public static Frame COMMENT_HASTAG_TAG;
  public static Frame COMPANY;
  public static Frame COMPANY_ISLOCATEDIN_COUNTRY;
  public static Frame CONTINENT;
  public static Frame COUNTRY;
  public static Frame COUNTRY_ISPARTOF_CONTINENT;
  public static Frame FORUM;
  public static Frame FORUM_HASMEMBER_PERSON;
  public static Frame FORUM_HASTAG_TAG;
  public static Frame PERSON;
  public static Frame PERSON_HASINTEREST_TAG;
  public static Frame PERSON_KNOWS_PERSON;
  public static Frame PERSON_LIKES_COMMENT;
  public static Frame PERSON_LIKES_POST;
  public static Frame PERSON_STUDYAT_UNIVERSITY;
  public static Frame PERSON_WORKAT_COMPANY;
  public static Frame POST;
  public static Frame POST_HASTAG_TAG;
  public static Frame TAGCLASS;
  public static Frame TAG;
  public static Frame UNIVERSITY;
  public static Frame UNIVERSITY_ISLOCATEDIN_CITY;
  private static long NSIZE, FSIZE; // Size on disk, in-memory

  // Some pre-built relationships.

  // Person-knows-person.  Hashed by person# to a sparse set of person#s.  Symmetric.
  public static NonBlockingHashMapLong<SparseBitSet> P_KNOWS_P;
  public static NonBlockingHashMapLong<Long> CITY_COUNTRY;
  
  public static void main( String[] args ) throws IOException {
    H2O.main(new String[0]);

    // ------------
    // Load all the CSVs
    long t0 = System.currentTimeMillis(), t;
    System.out.println("Loading TSMB data for "+SCALE_FACTOR);

    CITY = load("City");
    //CITY_ISPARTOF_COUNTRY = load("City_isPartOf_Country");
    COMMENT = load("Comment");
    COMMENT_HASTAG_TAG = load("Comment_hasTag_Tag");
    //COMPANY = load("Company");
    //COMPANY_ISLOCATEDIN_COUNTRY = load("Company_isLocatedIn_Country");
    //CONTINENT = load("Continent");
    //COUNTRY = load("Country");
    //COUNTRY_ISPARTOF_CONTINENT = load("Country_isPartOf_Continent");
    //FORUM = load("Forum");
    //FORUM_HASMEMBER_PERSON = load("Forum_hasMember_Person");
    //FORUM_HASTAG_TAG = load("Forum_hasTag_Tag");
    PERSON = load("Person");
    PERSON_HASINTEREST_TAG = load("Person_hasInterest_Tag");
    PERSON_KNOWS_PERSON = load("Person_knows_Person");
    //PERSON_LIKES_COMMENT = load("Person_likes_Comment");
    //PERSON_LIKES_POST = load("Person_likes_Post");
    //PERSON_STUDYAT_UNIVERSITY = load("Person_studyAt_University");
    //PERSON_WORKAT_COMPANY = load("Person_workAt_Company");
    POST = load("Post");
    POST_HASTAG_TAG = load("Post_hasTag_Tag");
    //TAGCLASS = load("TagClass");
    //TAG = load("Tag");
    //UNIVERSITY = load("University");
    //UNIVERSITY_ISLOCATEDIN_CITY = load("University_isLocatedIn_City");
    //System.out.println(H2O.STOREtoString());    
    t = System.currentTimeMillis(); System.out.println("Data loaded; "+PrettyPrint.bytes(NSIZE)+" bytes in "+(t-t0)+" msec, Frames take "+PrettyPrint.bytes(FSIZE)); t0=t;

    // ------------
    // Build some shared common relationships.
    
    // Build person-knows-person as a hashtable from person# to a (hashtable of person#s).
    // Symmetric.  2nd table is a sparse bitmap (no value).
    Vec p1s = PERSON_KNOWS_PERSON.vec("person1id");
    Vec p2s = PERSON_KNOWS_PERSON.vec("person2id");
    P_KNOWS_P = new BuildP1P2().doAll(p1s,p2s)._p1p2s;

    // Hash from city to country
    CITY_COUNTRY = new NonBlockingHashMapLong<>();
    Vec city = CITY.vec("id");
    Vec cnty = CITY.vec("ispartof_country");
    Vec.Reader vrcity = city.new Reader();
    Vec.Reader vrcnty = cnty.new Reader();
    for( int i=0; i<vrcity.length(); i++ )
      CITY_COUNTRY.put((long)vrcity.at8(i),(Long)vrcnty.at8(i));
    
    t = System.currentTimeMillis(); System.out.println("Building shared hashes in "+(t-t0)+" msec"); t0=t;

    // ------------
    // Run all queries once
    TSMBI[] delves = new TSMBI[]{new TSMB1(), new TSMB5(),new TSMB6()};
    //TSMBI[] delves = new TSMBI[]{new TSMB6()}; // DEBUG one query
    System.out.println("--- Run Once ---");
    for( TSMBI query : delves ) {
      System.out.println("--- "+query.name()+" ---");
      long cnt = query.run();
      System.out.println(cnt);
      t = System.currentTimeMillis(); System.out.println("--- "+query.name()+" "+(t-t0)+" msec ---"); t0=t;
    }

    System.out.println("--- Run Many ---");
    for( TSMBI query : delves ) {
      System.out.print(query.name()+" ");
      for( int i=0; i<10; i++ ) {
        query.run();
        t = System.currentTimeMillis(); System.out.print(""+(t-t0)+" msec, "); t0=t;
      }
      System.out.println();
    }                   
    System.out.println();

    // Leak detection
    CITY.delete();
    COMMENT.delete();
    COMMENT_HASTAG_TAG.delete();
    PERSON.delete();
    PERSON_HASINTEREST_TAG.delete();
    PERSON_KNOWS_PERSON.delete();
    POST.delete();
    POST_HASTAG_TAG.delete();
    //System.out.println(H2O.STOREtoString());
    
    System.exit(0);
  }
  public interface TSMBI { long run(); String name(); }

  public static Frame load(String fname) throws IOException {
    NFSFileVec nfs = NFSFileVec.make(DIRNAME+fname+".csv");
    NSIZE += nfs.length();
    Key<?>[] keys = new Key[]{nfs._key};
    // Force CSV parse, with '|' field separator, no-single-quotes, headers,
    // column count from headers, no heuristic data (yet), no errors (yet).
    ParseSetup guess1 = new ParseSetup(DefaultParserProviders.CSV_INFO, (byte)'|', false, 1, -1, null, new ParseWriter.ParseErr[0]);
    ParseSetup guess2 = ParseSetup.guessSetup(keys, guess1);
    Frame fr = ParseDataset.parse(Key.make(fname+".hex"), keys, true, guess2);
    FSIZE += fr.byteSize();
    return fr;
  }

  private static class BuildP1P2 extends MRTask<BuildP1P2> {
    transient NonBlockingHashMapLong<SparseBitSet> _p1p2s;
    @Override protected void setupLocal() { _p1p2s = new NonBlockingHashMapLong<>((int)(_fr.numRows()*2)); }
    @Override public void map(Chunk p1s, Chunk p2s ) {
      for( int i=0; i<p1s._len; i++ ) {
        long p1 = p1s.at8(i);
        long p2 = p2s.at8(i);
        build_hash(_p1p2s,p1,p2);
        build_hash(_p1p2s,p2,p1);
      }      
    }
    @Override public void reduce( BuildP1P2 bld ) {
      if( _p1p2s != bld._p1p2s )
        throw new RuntimeException("distributed reduce not implemented");
    }
  }

  static void build_hash(NonBlockingHashMapLong<SparseBitSet> sbsis, long c0, long c1) {
    SparseBitSet sbsi = sbsis.get(c0);
    if( sbsi==null ) {
      sbsis.putIfAbsent(c0,new SparseBitSet(32));
      sbsi = sbsis.get(c0);
    }
    sbsi.set(c1);               // Sparse-bit-set
  }

  // Summary printer for hash-of-hashes.
  static void print(String msg, NonBlockingHashMapLong<SparseBitSet> p2xs) {
    long sum=0,sum2=0;
    double sum_probe_ratio=0, max_probe_ratio=-1, sum_probes=0, max_probes=-1;
    long min=Long.MAX_VALUE;
    long max=0;
    SparseBitSet bad=null;
    int probe_cnt=0;
    for( SparseBitSet p2x : p2xs.values() ) {
      long size = p2x.fast_cardinality();
      sum  += size;
      sum2 += size*size;
      if( size < min ) min = size;
      if( size > max ) max = size;
      //// Check hash goodness
      //long probes = p2x.reprobes();
      //long tsts   = p2x.tsts    ();
      //if( tsts>0 ) {
      //  probe_cnt++;
      //  double ratio = (double)probes/tsts;
      //  sum_probes += probes;
      //  sum_probe_ratio += ratio;
      //  if( probes > max_probes ) max_probes = probes;
      //  if( ratio > max_probe_ratio ) max_probe_ratio = ratio;
      //  if( ratio > 15 ) bad=p2x;
      //}
    }
    long size = p2xs.size();
    double avg = (double)sum/size;
    double std = Math.sqrt((double)sum2/size);
    double avg_probes = sum_probes/probe_cnt;
    double avg_probe_ratio = sum_probe_ratio/probe_cnt;
    System.out.println(msg+": "+size+", avg "+avg+", min "+min+", max "+max+", stddev "+std);
    //System.out.println(msg+": "+size+", avg "+avg+", min "+min+", max "+max+", stddev "+std+", avg_probes "+avg_probes+", max_probes "+max_probes+", avg_probe_ratio "+avg_probe_ratio+", max_probe_ratio "+max_probe_ratio);
    if( bad!=null )
      System.out.println(" bad size "+bad.cardinality()+", table size "+bad.rawKeySet().length);
  }


  // Summary printer for hash-of-hashes.
  static void printA(String msg, NonBlockingHashMapLong<AryInt> p2xs) {
    long sum=0,sum2=0;
    long min=Long.MAX_VALUE;
    long max=0;
    for( AryInt p2x : p2xs.values() ) {
      int size = p2x._len;
      sum  += size;
      sum2 += size*size;
      if( size < min ) min = size;
      if( size > max ) max = size;
    }
    long size = p2xs.size();
    double avg = (double)sum/size;
    double std = Math.sqrt((double)sum2/size);
    System.out.println(msg+": "+size+", avg="+avg+", min="+min+", max="+max+", stddev="+std);
  }
}
