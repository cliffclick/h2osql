package org.cliffc.sql;

import org.joda.time.DateTime;
import water.*;
import water.fvec.*;
import water.rapids.Merge;
import water.nbhm.NonBlockingHashMapLong;
import water.util.SB;
import java.util.Arrays;

/**
def q11 = count[person1, person2, person3:
    person_knows_person(person1, person2)
    and person_knows_person(person2, person3)
    and person_knows_person(person1, person3)
]

            Answer    H2O 20CPU   DOVE0
SF0.1:      200280    0.000 sec   5.350 sec
SF1  :     3107478    0.015 sec
SF10 :    37853736    0.283 sec
SF100:   487437702    4.365 sec
                      3.930 sec using 32bit person ids
*/

public class TSMB11 implements TSMB.TSMBI {
  @Override public String name() { return "TSMB11"; }
  static final boolean PRINT_TIMING = false;
  
  // -----------------------------------------------------------------
  // Do triangles the "obvious" H2O way; parallelize edges; at one node, walk
  // all outgoing edges and look for a hit.  Requires all edges in a vector
  // form, and also a sparse adjacency matrix (hash of sparse edges) which fits
  // on one node.
  @Override public long run() {
    long t0 = System.currentTimeMillis(), t;

    // Count on dense numbers
    // ForAll P1s...
    Vec p1s = TSMB.PERSON_KNOWS_PERSON.vec("dp1");
    Vec p2s = TSMB.PERSON_KNOWS_PERSON.vec("dp2");
    long cnt = new CountI().doAll(p1s,p2s)._cnt;
    if( PRINT_TIMING ) { t=System.currentTimeMillis(); System.out.println("ForAll P1,P2,P3 "+(t-t0)+" msec"); t0=t; }
    
    return cnt;
  }

  private static class CountI extends MRTask<CountI> {
    long _cnt;
    @Override public void map( Chunk p1s, Chunk p2s ) {
      long cnt=0;
      for( int i=0; i<p1s._len; i++ ) {
        int p1 = (int)p1s.at8(i), p2 = (int)p2s.at8(i);
        SparseBitSetInt p1ks = TSMB.P_KNOWS_P.get(p1);
        SparseBitSetInt p2ks = TSMB.P_KNOWS_P.get(p2);
        // Walk the smaller, test against larger.  Worth about 1.67x speedup at
        // smaller scales, 1.25x at SF100.
        if( p1ks.fast_cardinality() < p2ks.fast_cardinality() )
          { SparseBitSetInt tmp = p1ks; p1ks = p2ks; p2ks = tmp; }
        for( int p3 : p2ks.rawKeySet() )
          if( p3!=0 && p1ks.tst(p3) ) // p1 knowns p3 also
            cnt+=2;             // twice, because triangulation
      }
      _cnt=cnt;
    }
    @Override public void reduce( CountI C ) { _cnt += C._cnt; }
  }
}
