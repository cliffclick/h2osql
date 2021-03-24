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

Umbra,                             H2O
SF0.1:   0.3401sec,    51009398    0.03sec
SF1  :  22.4546sec,  1596153418    4.2sec
SF10 : 303.2664sec, 22851049394    7.6sec
*/

public class Delve6 implements TSMB.Delve {
  @Override public String name() { return "Delve6"; }
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
    
    // Person Knows Persons.
    // Restructure to hash-of-hashes, or hash-of-(sparse_bit_set).
    Vec p1s = TSMB.PERSON_KNOWS_PERSON.vec("person1id");
    Vec p2s = TSMB.PERSON_KNOWS_PERSON.vec("person2id");
    //NonBlockingHashMapLong<NonBlockingHashMapLong> p1p2s = build_p1p2s("P1P2s",p1s,p2s);
    NonBlockingHashMapLong<NonBlockingHashMapLong> p1p2s = new BuildP1P2().doAll(p1s,p2s)._p1p2s;
    //print("P1P2s",p1p2s);
    if( PRINT_TIMING ) { t=System.currentTimeMillis(); System.out.println("Restructure P1P2 "+(t-t0)+" msec"); t0=t; }

    // Person #Tags
    // Restructure to hash-of-hashes, or hash-of-(sparse_bit_set).
    Vec vid = TSMB.PERSON_HASINTEREST_TAG.vec("id");
    Vec vtg = TSMB.PERSON_HASINTEREST_TAG.vec("hasinterest_tag");
    NonBlockingHashMapLong<NonBlockingHashMapLong> ptags = build_tags("PTAGs",vid,vtg);
    if( PRINT_TIMING ) { t=System.currentTimeMillis(); System.out.println("Restructure P1P2 "+(t-t0)+" msec"); t0=t; }

    // ForAll P1s...
    long cnt = new Count(p1p2s,ptags).doAll(p1s,p2s)._cnt;
    if( PRINT_TIMING ) { t=System.currentTimeMillis(); System.out.println("ForAll P1,P2,P3 "+(t-t0)+" msec"); t0=t; }
    
    return cnt;
  }
  
  private static NonBlockingHashMapLong<NonBlockingHashMapLong> build_p1p2s(String msg, Vec col0, Vec col1) {
    NonBlockingHashMapLong<NonBlockingHashMapLong> nbhms = new NonBlockingHashMapLong<>();
    Vec.Reader vr0 = col0.new Reader();
    Vec.Reader vr1 = col1.new Reader();
    for( int i=0; i<vr0.length(); i++ ) {
      long c0 = vr0.at8(i), c1 = vr1.at8(i);
      _build(nbhms,c0,c1);
      _build(nbhms,c1,c0);
    }
    print(msg,nbhms);
    return nbhms;
  }

  private static class BuildP1P2 extends MRTask<BuildP1P2> {
    transient NonBlockingHashMapLong<NonBlockingHashMapLong> _p1p2s;
    @Override protected void setupLocal() { _p1p2s = new NonBlockingHashMapLong<NonBlockingHashMapLong>(10000); }
    @Override public void map(Chunk p1s, Chunk p2s ) {
      for( int i=0; i<p1s._len; i++ ) {
        long p1 = p1s.at8(i);
        long p2 = p2s.at8(i);
        _build(_p1p2s,p1,p2);
        _build(_p1p2s,p2,p1);
      }      
    }
    @Override public void reduce( BuildP1P2 bld ) {
      if( _p1p2s != bld._p1p2s )
        throw new RuntimeException("distributed reduce not implemented");
    }
  }
  
  private static NonBlockingHashMapLong<NonBlockingHashMapLong> build_tags(String msg, Vec col0, Vec col1) {
    NonBlockingHashMapLong<NonBlockingHashMapLong> nbhms = new NonBlockingHashMapLong<>();
    Vec.Reader vr0 = col0.new Reader();
    Vec.Reader vr1 = col1.new Reader();
    for( int i=0; i<vr0.length(); i++ )
      _build(nbhms,vr0.at8(i),vr1.at8(i));
    //print(msg,nbhms);
    return nbhms;
  }

  private static void _build(NonBlockingHashMapLong<NonBlockingHashMapLong> nbhms, long c0, long c1) {
    NonBlockingHashMapLong nbhm = nbhms.get(c0);
    if( nbhm==null ) {
      nbhms.putIfAbsent(c0,new NonBlockingHashMapLong());
      nbhm = nbhms.get(c0);
    }
    nbhm.put(c1,"");         // Sparse-bit-set, just a hash with no value payload
  }

  private static class Count extends MRTask<Count> {
    long _cnt;
    final NonBlockingHashMapLong<NonBlockingHashMapLong> _p1p2s, _ptags;
    Count( NonBlockingHashMapLong<NonBlockingHashMapLong> p1p2s, NonBlockingHashMapLong<NonBlockingHashMapLong> ptags ) { _p1p2s = p1p2s; _ptags=ptags; }
    @Override public void map( Chunk p1s, Chunk p2s ) {
      long cnt=0;
      for( int i=0; i<p1s._len; i++ ) {
        long p1 = p1s.at8(i), p2 = p2s.at8(i);
        cnt += _check_knows(p1,p2) + _check_knows(p2,p1);
      }
      _cnt=cnt;
    }
    // Check P1->P2->P3, P1!=P3, P1-!->P3 & count P3 tags.
    private long _check_knows(long p1, long p2) {
      NonBlockingHashMapLong p3s = _p1p2s.get(p2);
      long cnt=0;
      for( long p3 : p3s.keySetLong() )
        if( p1!=p3 ) {
          NonBlockingHashMapLong<NonBlockingHashMapLong> p1s = _p1p2s.get(p1);
          if( p1s==null || !p1s.containsKey(p3) )
            cnt += _ptags.get(p3).size();
        }
      return cnt;
    }
    @Override public void reduce( Count C ) { _cnt += C._cnt; }
  }
  
  private static void print(String msg, NonBlockingHashMapLong<NonBlockingHashMapLong> p2xs) {
    long sum=0,sum2=0;
    long min=Long.MAX_VALUE;
    long max=0;
    for( NonBlockingHashMapLong p2x : p2xs.values() ) {
      long size = p2x.size();
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
