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

Umbra, 
SF0.1:   0.3401sec,    51009398
SF1  :  22.4546sec,  1596153418
SF10 : 303.2664sec, 22851049394
*/

public class Delve6 implements TSMB.Delve {
  @Override public String name() { return "Delve6"; }
  static final boolean PRINT_TIMING = false;

  // Query plan:

  // Flip person->person to hash-of-hashs.
  // Compute hash of Persons with interest tags
  // ForAll P1s
  //   ForAll P1.P2s
  //     ForAll P2.P3s
  //       if( P1!=P3 && !P1.hash(P3) )
  //         count += P3.#interest_tags;
  
  
  @Override public long run() {
    long t0 = System.currentTimeMillis(), t;
    
    // Person Knows Persons.
    // Restructure to hash-of-hashes, or hash-of-(sparse_bit_set).
    Vec p1 = TSMB.PERSON_KNOWS_PERSON.vec("person1id");
    Vec p2 = TSMB.PERSON_KNOWS_PERSON.vec("person2id");
    NonBlockingHashMapLong<NonBlockingHashMapLong> p1p2s = hash_of_hash("P1P2s",p1,p2);
    if( PRINT_TIMING ) { t=System.currentTimeMillis(); System.out.println("Restructure P1P2 "+(t-t0)+" msec"); t0=t; }

    // Person #Tags
    // Restructure to hash-of-hashes, or hash-of-(sparse_bit_set).
    Vec vid = TSMB.PERSON_HASINTEREST_TAG.vec("id");
    Vec vtg = TSMB.PERSON_HASINTEREST_TAG.vec("hasinterest_tag");
    NonBlockingHashMapLong<NonBlockingHashMapLong> ptags = hash_of_hash("PTAGs",vid,vtg);
    if( PRINT_TIMING ) { t=System.currentTimeMillis(); System.out.println("Restructure P1P2 "+(t-t0)+" msec"); t0=t; }

    // ForAll P1s...
    long cnt = new Count(p1p2s,ptags).doAll(p1,p2)._cnt;
    if( PRINT_TIMING ) { t=System.currentTimeMillis(); System.out.println("ForAll P1,P2,P3 "+(t-t0)+" msec"); t0=t; }
    
    return cnt;
  }

  NonBlockingHashMapLong<NonBlockingHashMapLong> hash_of_hash(String msg, Vec col0, Vec col1) {
    NonBlockingHashMapLong<NonBlockingHashMapLong> nbhms = new NonBlockingHashMapLong<>();
    Vec.Reader vr0 = col0.new Reader();
    Vec.Reader vr1 = col1.new Reader();
    for( int i=0; i<vr0.length(); i++ ) {
      long c0 = vr0.at8(i);
      long c1 = vr1.at8(i);
      NonBlockingHashMapLong nbhm = nbhms.get(c0);
      if( nbhm==null ) {
        nbhms.putIfAbsent(c0,new NonBlockingHashMapLong());
        nbhm = nbhms.get(c0);
      }
      nbhm.put(c1,"");         // Sparse-bit-set, just a hash with no value payload
    }
    print(msg,nbhms);
    return nbhms;
  }

  
  private static class Count extends MRTask<Count> {
    long _cnt;
    final NonBlockingHashMapLong<NonBlockingHashMapLong> _p1p2s, _ptags;
    Count( NonBlockingHashMapLong<NonBlockingHashMapLong> p1p2s, NonBlockingHashMapLong<NonBlockingHashMapLong> ptags ) { _p1p2s = p1p2s; _ptags=ptags; }
    @Override public void map( Chunk p1s, Chunk p2s ) {
      long cnt=0;
      for( int i=0; i<p1s._len; i++ ) {
        long p1 = p1s.at8(i), p2 = p2s.at8(i);
        NonBlockingHashMapLong p3s = _p1p2s.get(p2);
        if( p3s != null )
          for( long p3 : p3s.keySetLong() )
            if( p1!=p3 && !_p1p2s.get(p1).containsKey(p3) ) {
              NonBlockingHashMapLong tags = _ptags.get(p3);
              if( tags != null )
                cnt += tags.size();
            }
      }
      _cnt=cnt;
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
