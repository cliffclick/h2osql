package org.cliffc.sql;

import water.*;
import water.fvec.*;
import org.joda.time.DateTime;
import water.nbhm.NonBlockingHashMapLong;

/**
def q6 = count[person1, person2, person3, tag:
    person_knows_person(person1, person2)
    and person_knows_person(person2, person3)
    and person_has_interest_tag(person3, tag)
    and person1 != person3
    and not(person_knows_person(person1, person3))
]

            Answer  Umbra 1 thrd  Umbra 48thrd   H2O 20thrd
SF0.1:    51009398    0.3401 sec    0.0298 sec     0.03  sec
SF1  :  1596153418   22.4546 sec    0.7598 sec     0.265 sec
SF10 : 22851049394  303.2664 sec   10.4135 sec     7.030 sec
*/

public class TSMB6 implements TSMB.TSMBI {
  @Override public String name() { return "TSMB6"; }
  static final boolean PRINT_TIMING = false;

  // Query plan:
  // Extra info: person-knows-person is symmetric.
  
  // Flip person->person to hash-of-hashs.
  // Compute hash of Persons with interest tags
  // ForAll P1s
  //   ForAll P1.P2s (and the inverted case)
  //     ForAll P2.P3s
  //       if( P1!=P3 && !P1.hash(P3) )
  //         count += P3.#interest_tags;
  
  @Override public long run() {
    long t0 = System.currentTimeMillis(), t;
    
    // Person #Tags
    // Restructure to hash-of-hashes, or hash-of-(sparse_bit_set).
    Vec vid = TSMB.PERSON_HASINTEREST_TAG.vec("id");
    Vec vtg = TSMB.PERSON_HASINTEREST_TAG.vec("hasinterest_tag");
    NonBlockingHashMapLong<NonBlockingHashMapLong> ptags = new BuildTags().doAll(vid,vtg)._tags;
    if( PRINT_TIMING ) { t=System.currentTimeMillis(); System.out.println("Restructure P1P2#"+ptags.size()+" "+(t-t0)+" msec"); t0=t; }

    // ForAll P1s...
    Vec p1s = TSMB.PERSON_KNOWS_PERSON.vec("person1id");
    Vec p2s = TSMB.PERSON_KNOWS_PERSON.vec("person2id");
    long cnt = new Count(TSMB.P_KNOWS_P,ptags).doAll(p1s,p2s)._cnt;
    if( PRINT_TIMING ) { t=System.currentTimeMillis(); System.out.println("ForAll P1,P2,P3 "+(t-t0)+" msec"); t0=t; }
    
    return cnt;
  }

  private static class BuildTags extends MRTask<BuildTags> {
    transient NonBlockingHashMapLong<NonBlockingHashMapLong> _tags;
    @Override protected void setupLocal() { _tags = new NonBlockingHashMapLong<NonBlockingHashMapLong>((int)(_fr.numRows()>>3)); }
    @Override public void map(Chunk cids, Chunk ctags) {
      for( int i=0; i<cids._len; i++ )
        TSMB.build_hash(_tags,cids.at8(i),ctags.at8(i));        
    }
    @Override public void reduce( BuildTags bld ) {
      if( _tags != bld._tags )
        throw new RuntimeException("distributed reduce not implemented");
    }
  }

  private static class Count extends MRTask<Count> {
    long _cnt;
    final NonBlockingHashMapLong<NonBlockingHashMapLong> _p1p2s, _ptags;
    Count( NonBlockingHashMapLong<NonBlockingHashMapLong> p1p2s, NonBlockingHashMapLong<NonBlockingHashMapLong> ptags ) { _p1p2s = p1p2s; _ptags=ptags; }
    @Override public void map( Chunk p1s, Chunk p2s ) {
      long cnt=0;
      for( int i=0; i<p1s._len; i++ ) {
        long p1 = p1s.at8(i), p2 = p2s.at8(i);
        NonBlockingHashMapLong<NonBlockingHashMapLong> np1s = _p1p2s.get(p1);
        NonBlockingHashMapLong<NonBlockingHashMapLong> np2s = _p1p2s.get(p2);        
        cnt += _check_knows(p1,np1s,np2s) + _check_knows(p2,np2s,np1s);
      }
      _cnt=cnt;
    }
    // Check P1->P2->P3, P1!=P3, P1-!->P3 & count P3 tags.
    private long _check_knows(long p1, NonBlockingHashMapLong<NonBlockingHashMapLong> p1s, NonBlockingHashMapLong p3s) {
      long cnt=0;
      for( long p3 : p3s.rawKeySet() )
        if( p3!=0 && p1!=p3 &&
            (p1s==null || !p1s.containsKey(p3)) )
          cnt += _ptags.get(p3).estimate_size();
      return cnt;
    }
    @Override public void reduce( Count C ) { _cnt += C._cnt; }
  }
}
