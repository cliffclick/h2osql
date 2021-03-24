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
  public static final String SCALE_FACTOR = "sf0.1";
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
  private static int NSIZE, FSIZE; // Size on disk, in-memory

  // Some pre-built relationships.

  // Person-knows-person.  Hashed by person# to a sparse set of person#s.  Symmetric.
  public static NonBlockingHashMapLong<NonBlockingHashMapLong> P_KNOWS_P;
  
  public static void main( String[] args ) throws IOException {
    H2O.main(new String[0]);

    // ------------
    // Load all the CSVs
    long t0 = System.currentTimeMillis(), t;
    System.out.println("Loading TPCH data for "+SCALE_FACTOR);

    //CITY = load("City");
    CITY_ISPARTOF_COUNTRY = load("City_isPartOf_Country");
    //COMMENT = load("Comment");
    //COMMENT_HASTAG_TAG = load("Comment_hasTag_Tag");
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
    //POST = load("Post");
    //POST_HASTAG_TAG = load("Post_hasTag_Tag");
    //TAGCLASS = load("TagClass");
    //TAG = load("Tag");
    //UNIVERSITY = load("University");
    //UNIVERSITY_ISLOCATEDIN_CITY = load("University_isLocatedIn_City");
    t = System.currentTimeMillis(); System.out.println("Data loaded; "+PrettyPrint.bytes(NSIZE)+" bytes in "+(t-t0)+" msec, Frames take "+PrettyPrint.bytes(FSIZE)); t0=t;

    // ------------
    // Build some shared common relationships.
    
    // Build person-knows-person as a hashtable from person# to a (hashtable of person#s).
    // Symmetric.  2nd table is a sparse bitmap (no value).
    Vec p1s = PERSON_KNOWS_PERSON.vec("person1id");
    Vec p2s = PERSON_KNOWS_PERSON.vec("person2id");
    P_KNOWS_P = new BuildP1P2().doAll(p1s,p2s)._p1p2s;    
    t = System.currentTimeMillis(); System.out.println("Building shared hashes in "+(t-t0)+" msec"); t0=t;

    // ------------
    // Run all queries once
    //TSMBI[] delves = new TSMBI[]{new TSMB5(),new TSMB6()};
    TSMBI[] delves = new TSMBI[]{new TSMB5()}; // DEBUG one query
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
      for( int i=0; i<5; i++ ) {
        query.run();
        t = System.currentTimeMillis(); System.out.print(""+(t-t0)+" msec, "); t0=t;
      }
      System.out.println();
    }                   
    System.out.println();
    
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
    transient NonBlockingHashMapLong<NonBlockingHashMapLong> _p1p2s;
    @Override protected void setupLocal() { _p1p2s = new NonBlockingHashMapLong<NonBlockingHashMapLong>(10000); }
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

  static void build_hash(NonBlockingHashMapLong<NonBlockingHashMapLong> nbhms, long c0, long c1) {
    NonBlockingHashMapLong nbhm = nbhms.get(c0);
    if( nbhm==null ) {
      nbhms.putIfAbsent(c0,new NonBlockingHashMapLong());
      nbhm = nbhms.get(c0);
    }
    nbhm.put(c1,"");         // Sparse-bit-set, just a hash with no value payload
  }

}
