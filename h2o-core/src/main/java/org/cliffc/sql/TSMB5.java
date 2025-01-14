package org.cliffc.sql;

import water.*;
import water.fvec.*;
import org.joda.time.DateTime;
import water.nbhm.NonBlockingHashMapLong;

/**
def q5 = count[cityA, cityB, cityC, country, pA, pB, pC:
    city_is_part_of_country(cityA, country)
    and city_is_part_of_country(cityB, country)
    and city_is_part_of_country(cityC, country)

    and person_is_located_in_place(pA, cityA)
    and person_is_located_in_place(pB, cityB)
    and person_is_located_in_place(pC, cityC)

    and person_knows_person(pA, pB)
    and person_knows_person(pB, pC)
    and person_knows_person(pC, pA)
]

          Answer  Umbra 1 thrd  Umbra 48thrd   H2O 20thrd
SF0.1:     30456     0.117 sec    0.0349 sec    0.005 sec
SF1  :    753570    2.3345 sec    0.2094 sec    0.016 sec
SF10 :  15028644   25.6234 sec    1.1867 sec    0.400 sec
SF100: 255675126                                6.800 sec
*/

public class TSMB5 implements TSMB.TSMBI {
  @Override public String name() { return "TSMB5"; }
  static final boolean PRINT_TIMING = false;
  
  // Extra info: person-knows-person is symmetric.
  // find triangles A<->B<->C<->A, where all 3 are in same country.

  // Query plan:
  // Hash person->country
  // ForAll P1s
  //   Get country
  //   ForAll P1.P2s
  //     Check same country
  //     ForAll P1.P2.P3s
  //       check P1.P3 && same country
  
  @Override public long run() {
    long t0 = System.currentTimeMillis(), t;

    Vec vper = TSMB.PERSON.vec("did");
    Vec vloc = TSMB.PERSON.vec("islocatedin_place");
    NonBlockingHashMapLong<Integer> p2c = new BuildP2C().doAll(vper,vloc)._p2c;
    if( PRINT_TIMING ) { t=System.currentTimeMillis(); System.out.println("Hash p->country "+(t-t0)+" msec"); t0=t; }
    
    // ForAll P1s...
    Vec p1s = TSMB.PERSON_KNOWS_PERSON.vec("dp1");
    Vec p2s = TSMB.PERSON_KNOWS_PERSON.vec("dp2");
    long cnt = new Count(p2c).doAll(p1s,p2s)._cnt;
    if( PRINT_TIMING ) { t=System.currentTimeMillis(); System.out.println("ForAll P1,P2,P3 "+(t-t0)+" msec"); t0=t; }
    
    return cnt;
  }

  private static class Count extends MRTask<Count> {
    long _cnt;
    final NonBlockingHashMapLong<Integer> _p2c;
    Count( NonBlockingHashMapLong<Integer> p2c ) { _p2c=p2c; }
    @Override public void map( Chunk p1s, Chunk p2s ) {
      long cnt=0;
      for( int i=0; i<p1s._len; i++ ) {
        int p1 = (int)p1s.at8(i), p2 = (int)p2s.at8(i);
        Integer country = _p2c.get(p1);
        if( _p2c.get(p2)!=country ) continue; // p1,p2 not same country
        SparseBitSetInt p3s = TSMB.P_KNOWS_P.get(p2);
        for( int p3 : p3s.rawKeySet() )
          if( p3!=0 && _p2c.get(p3)==country && TSMB.P_KNOWS_P.get(p1).tst(p3) ) // p1 knowns p3 also; p3 same country
            cnt+=2;             // twice, because triangulation
      }
      _cnt=cnt;
    }
    @Override public void reduce( Count C ) { _cnt += C._cnt; }
  }
  
  private static class BuildP2C extends MRTask<BuildP2C> {
    transient NonBlockingHashMapLong<Integer> _p2c;
    @Override protected void setupLocal() { _p2c = new NonBlockingHashMapLong<Integer>(); }
    @Override public void map( Chunk pers, Chunk citys ) {
      for( int i=0; i<pers._len; i++ )
        _p2c.put(pers.at8(i),TSMB.CITY_COUNTRY.get(citys.at8(i)));
    }
    @Override public void reduce( BuildP2C bld ) {
      if( _p2c != bld._p2c )
        throw new RuntimeException("distributed reduce not implemented");
    }
  }


}
