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
SF0.1:    51009398    0.3401 sec    0.0298 sec     0.010 sec
SF1  :  1596153418   22.4546 sec    0.7598 sec     0.120 sec
SF10 : 22851049394  303.2664 sec   10.4135 sec     1.680 sec
SF100:280039991566                               160     sec Plan#1 (Sparse Bit Set for tags)
SF100:                                            29.5   sec Plan#2 (Count Tags)
SF100:                                                   sec Plan#2 + (NB SBS for P2P)

*/

public class TSMB6 implements TSMB.TSMBI {
  @Override public String name() { return "TSMB6"; }
  static final boolean PRINT_TIMING = false;

  // pass1: Count tags
  // pass2: ForAll P2's; sum += multiple #P1.tags)*#P3.tags
  // err1: ForAll P1's; sum += #P1P2*#P1.tags
  // err2: ForAll P1's,
  //         Put P2s on sorted worklist, small to large
  //           ForAll P1.P2's
  //             if( P1.has(P1.P2.P3) )
  //                sum += tags
  //                toss out P3 from worklist, never walk the largest P3s
  //         //ForAll P1.P2's,
  //         //  if( P1.has(P1.P2.P3) ) sum += tags
  // Result pass2-err1-err2

  // Query plan#2:
  //   Same as plan#1, but count tags only, assuming no dup tags
  @Override public long run() {
    long t0 = System.currentTimeMillis(), t;
    
    // Person #Tags
    // Restructure to hash-of-hashes, or hash-of-(sparse_bit_set).
    Vec vid = TSMB.PERSON_HASINTEREST_TAG.vec("id");
    Vec vtg = TSMB.PERSON_HASINTEREST_TAG.vec("hasinterest_tag");
    NonBlockingHashMapLong<Long> ptags = new BuildsTags2().doAll(vid,vtg)._tags;
    if( PRINT_TIMING ) { t=System.currentTimeMillis(); System.out.println("Restructure P1P2#"+ptags.size()+" "+(t-t0)+" msec"); t0=t; }

    // ForAll P1s...
    Vec p1s = TSMB.PERSON_KNOWS_PERSON.vec("person1id");
    Vec p2s = TSMB.PERSON_KNOWS_PERSON.vec("person2id");
    long cnt = new Count2(ptags).doAll(p1s,p2s)._cnt;
    if( PRINT_TIMING ) { t=System.currentTimeMillis(); System.out.println("ForAll P1,P2,P3 "+(t-t0)+" msec"); t0=t; }
    
    return cnt;
  }
  
  private static class BuildsTags2 extends MRTask<BuildsTags2> {
    transient NonBlockingHashMapLong<Long> _tags;
    @Override protected void setupLocal() { _tags = new NonBlockingHashMapLong<Long>((int)(_fr.numRows()>>1)); }
    @Override public void map(Chunk cids, Chunk ctags) {
      for( int i=0; i<cids._len; i++ ) {
        long id = cids.at8(i);
        while( true ) {
          Long cnt0 = _tags.get(id);
          long cnt1 = (cnt0==null ? 0 : cnt0)+1;
          if( cnt0==null
              ? _tags.putIfAbsent(id,(Long)cnt1)==null
              : _tags.replace((long)id,cnt0,(Long)cnt1) ) break;
        }
      }
    }
    @Override public void reduce( BuildsTags2 bld ) {
      if( _tags != bld._tags )
        throw new RuntimeException("distributed reduce not implemented");
    }
  }

  private static class Count2 extends MRTask<Count2> {
    long _cnt;
    final NonBlockingHashMapLong<Long> _ptags;
    Count2( NonBlockingHashMapLong<Long> ptags ) { _ptags=ptags; }
    @Override public void map( Chunk p1s, Chunk p2s ) {
      long cnt=0;
      for( int i=0; i<p1s._len; i++ ) {
        long p1 = p1s.at8(i), p2 = p2s.at8(i);
        SparseBitSet np1s = TSMB.P_KNOWS_P.get(p1);
        SparseBitSet np2s = TSMB.P_KNOWS_P.get(p2);
        cnt += _check_knows(p1,np1s,np2s) + _check_knows(p2,np2s,np1s);
      }
      _cnt=cnt;
    }
    // Check P1->P2->P3, P1!=P3, P1-!->P3 & count P3 tags.
    private long _check_knows(long p1, SparseBitSet p1s, SparseBitSet p3s) {
      long cnt=0;
      for( long p3 : p3s.rawKeySet() )
        if( p3!=0 && p1!=p3 &&
            (p1s==null || !p1s.tst(p3)) )
          cnt += _ptags.get(p3);
      return cnt;
    }
    @Override public void reduce( Count2 C ) { _cnt += C._cnt; }
  }

  // -------------------------------
  // Query plan#1:
  // Extra info: person-knows-person is symmetric.
  
  // Flip person->person to hash-of-hashs.
  // Compute hash of Persons with interest tags
  // ForAll P1s
  //   ForAll P1.P2s (and the inverted case)
  //     ForAll P2.P3s
  //       if( P1!=P3 && !P1.hash(P3) )
  //         count += P3.#interest_tags;
  public long run1() {
    long t0 = System.currentTimeMillis(), t;
    
    // Person #Tags
    // Restructure to hash-of-hashes, or hash-of-(sparse_bit_set).
    Vec vid = TSMB.PERSON_HASINTEREST_TAG.vec("id");
    Vec vtg = TSMB.PERSON_HASINTEREST_TAG.vec("hasinterest_tag");
    NonBlockingHashMapLong<SparseBitSet> ptags = new BuildsTags1().doAll(vid,vtg)._tags;
    if( PRINT_TIMING ) { t=System.currentTimeMillis(); System.out.println("Restructure P1P2#"+ptags.size()+" "+(t-t0)+" msec"); t0=t; }

    // ForAll P1s...
    Vec p1s = TSMB.PERSON_KNOWS_PERSON.vec("person1id");
    Vec p2s = TSMB.PERSON_KNOWS_PERSON.vec("person2id");
    long cnt = new Count1(TSMB.P_KNOWS_P,ptags).doAll(p1s,p2s)._cnt;
    if( PRINT_TIMING ) { t=System.currentTimeMillis(); System.out.println("ForAll P1,P2,P3 "+(t-t0)+" msec"); t0=t; }
    
    return cnt;
  }

  private static class BuildsTags1 extends MRTask<BuildsTags1> {
    transient NonBlockingHashMapLong<SparseBitSet> _tags;
    @Override protected void setupLocal() { _tags = new NonBlockingHashMapLong<SparseBitSet>((int)(_fr.numRows()>>3)); }
    @Override public void map(Chunk cids, Chunk ctags) {
      for( int i=0; i<cids._len; i++ )
        TSMB.build_hash(_tags,cids.at8(i),ctags.at8(i));        
    }
    @Override public void reduce( BuildsTags1 bld ) {
      if( _tags != bld._tags )
        throw new RuntimeException("distributed reduce not implemented");
    }
  }

  private static class Count1 extends MRTask<Count1> {
    long _cnt;
    final NonBlockingHashMapLong<SparseBitSet> _p1p2s, _ptags;
    Count1( NonBlockingHashMapLong<SparseBitSet> p1p2s, NonBlockingHashMapLong<SparseBitSet> ptags ) { _p1p2s = p1p2s; _ptags=ptags; }
    @Override public void map( Chunk p1s, Chunk p2s ) {
      long cnt=0;
      for( int i=0; i<p1s._len; i++ ) {
        long p1 = p1s.at8(i), p2 = p2s.at8(i);
        SparseBitSet np1s = _p1p2s.get(p1);
        SparseBitSet np2s = _p1p2s.get(p2);        
        cnt += _check_knows(p1,np1s,np2s) + _check_knows(p2,np2s,np1s);
      }
      _cnt=cnt;
    }
    // Check P1->P2->P3, P1!=P3, P1-!->P3 & count P3 tags.
    private long _check_knows(long p1, SparseBitSet p1s, SparseBitSet p3s) {
      long cnt=0;
      for( long p3 : p3s.rawKeySet() )
        if( p3!=0 && p1!=p3 &&
            (p1s==null || !p1s.tst(p3)) )
          cnt += _ptags.get(p3).fast_cardinality();
      return cnt;
    }
    @Override public void reduce( Count1 C ) { _cnt += C._cnt; }
  }
}
