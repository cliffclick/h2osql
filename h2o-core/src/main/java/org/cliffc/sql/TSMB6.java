package org.cliffc.sql;

import org.joda.time.DateTime;
import water.*;
import water.fvec.*;
import water.nbhm.NonBlockingHashMapLong;
import java.util.Arrays;

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
SF100:                                            29.5   sec Plan#2 + (NB SBS for P2P)
SF100:                                            14.0   sec Plan#3 (overcount, then error-correct)
*/

public class TSMB6 implements TSMB.TSMBI {
  @Override public String name() { return "TSMB6"; }
  static final boolean PRINT_TIMING = false;

  // -------------------------------
  // Query plan#4

  // Does Not Work - because the 2nd-largest P3 might have error terms with the largest P3.
  // So still have to walk the 2nd-largest P3.
  
  // pass1: Count tags
  // pass2: ForAll P2's; sum += #P1.tags*#P3.tags
  // err3 : ForAll P1's; sum += #P1P2*#P1.tags
  // err4 : ForAll P1's,
  //          Put P2s on sorted worklist, small to large
  //            ForAll P1.P2's
  //              if( P1.has(P1.P2.P3) )
  //                 sum += tags
  //                 toss out P3 from worklist, never walk the largest P3s
  // Result is pass2-err3-err4
  public long run4() {
    long t0 = System.currentTimeMillis(), t;
    
    // Person #Tags
    // Restructure to hash-of-hashes, or hash-of-(sparse_bit_set).
    Vec vid = TSMB.PERSON_HASINTEREST_TAG.vec("id");
    Vec vtg = TSMB.PERSON_HASINTEREST_TAG.vec("hasinterest_tag");
    NonBlockingHashMapLong<Long> ptags = new BuildsTags2().doAll(vid,vtg)._tags;
    if( PRINT_TIMING ) { t=System.currentTimeMillis(); System.out.println("Restructure P1P2#"+ptags.size()+" "+(t-t0)+" msec"); t0=t; }

    // The next 3 passes can run in parallel
    
    // pass2: ForAll P2's; sum += #P1.tags*#P3.tags
    // This over-counts where p1==p3 or not(PKP(p1,p3)), but does not require a
    // loop over the PKP relation
    long cnt = new AllTags(ptags).doAll(TSMB.PERSON.vec("id"))._cnt;
    if( PRINT_TIMING ) { t=System.currentTimeMillis(); System.out.println("AllTags# "+cnt+" "+(t-t0)+" msec"); t0=t; }
    
    // err3 : ForAll P1's; sum += #P1P2*#P1.tags
    // Correct for p1==p3.
    long err3 = new Err3(ptags).doAll(TSMB.PERSON.vec("id"))._cnt;
    if( PRINT_TIMING ) { t=System.currentTimeMillis(); System.out.println("Err3# "+err3+" "+(t-t0)+" msec"); t0=t; }
    
    // err4 : ForAll P1's,
    // Correct for not(PKP(p1,p3))
    long err4 = new Err4X(ptags).doAll(TSMB.PERSON.vec("id"))._cnt;
    if( PRINT_TIMING ) { t=System.currentTimeMillis(); System.out.println("Err4# "+err4+" "+(t-t0)+" msec"); t0=t; }
    
    return cnt - err3 - err4;
  }
  
  // err4 : ForAll P1's,
  //          Put P2s on sorted worklist, small to large
  //            ForAll P1.P2's
  //              if( P1.has(P1.P2.P3) )
  //                 sum += P1.tags
  //                 toss out P3 from worklist, never walk the largest P3s
  // Correct for not(PKP(p1,p3))
  private static class Err4X extends MRTask<Err4X> {
    long _cnt;
    final NonBlockingHashMapLong<Long> _ptags;
    Err4X( NonBlockingHashMapLong<Long>  ptags ) { _ptags=ptags; }
    @Override public void map( Chunk pids ) {
      long cnt=0;
      SparseBitSet[] work = new SparseBitSet[1024];
      for( int i=0; i<pids._len; i++ ) {
        long p1 = pids.at8(i);
        SparseBitSet p2s = TSMB.P_KNOWS_P.get(p1); // Set of p2->p13s
        if( p2s != null ) {
          long np1cnt = _ptags.get(p1);

          // Weaker Plan#4: Do Not Walk only the largest guy.  Find the largest guy.
          long pmax = -1, pcard = -1;
          for( long p2 : p2s.rawKeySet() ) // ForAll P2s, find max
            if( p2 != 0 ) {
              long p2card = TSMB.P_KNOWS_P.get(p2).fast_cardinality();
              if( pcard < p2card ) { pmax = p2;  pcard = p2card; }
            }

          // Walk all but largest
          for( long p2 : p2s.rawKeySet() ) // ForAll P2s, do P1
            if( p2 != 0 && p2!= pmax ) {
              SparseBitSet p3s = TSMB.P_KNOWS_P.get(p2); // Set of p2->p3s
              for( long p3 : p3s.rawKeySet() )
                if( p3 != 0 && p2s.tst(p3) ) { // does p1 know p3?
                  cnt += np1cnt;
                  if( p3==pmax ) cnt += np1cnt;
                }
            }

          // INCORRECT: to not walk the larger sets, even as they get hits from
          // the smaller sets, because the 2nd-largest set can have hits on the
          // largest set.
          
          //// Copy p2s to a worklist
          //int len=0;
          //for( long p2 : p2s.rawKeySet() )
          //  if( p2 != 0 ) {
          //    if( len==work.length ) work = Arrays.copyOf(work,len<<1); // Double in size as needed
          //    work[len++] = TSMB.P_KNOWS_P.get(p2);
          //  }
          //
          //// Heapify/sort
          //for( int j = (len>>1)-1; j >= 0; j-- )
          //  heapify(work,len,j);
          //
          //// For all P2s, smallest to largest do...
          //while( len > 0 ) {
          //  SparseBitSet p3s = pop(work,0,len--); // Smallest set of p2->p3s
          //  for( long p3 : p3s.rawKeySet() )
          //    if( p3 != 0 && p2s.tst(p3) ) { // does p1 know p3?
          //      // lazy remove p3 from p2s
          //      SparseBitSet pkp3 = TSMB.P_KNOWS_P.get(p3);
          //      cnt += np1cnt + np1cnt;
          //      boolean found=false;
          //      for( int j=0; j<len; j++ )
          //        if( work[j]==pkp3 )
          //          { pop(work,j,len--); found=true; break; }
          //      assert found;                
          //    }
          //}
        }
      }
      _cnt=cnt;
    }
    @Override public void reduce( Err4X bld ) { _cnt += bld._cnt; }

    // Remove element; externally need to lower 'len'
    private static SparseBitSet pop( SparseBitSet[] work, int idx, int len ) {
      SparseBitSet rez = work[idx];
      work[idx] = work[len-1];
      heapify(work,len-1,idx);
      return rez;
    }
    
    // 'heapify' as in heap-sort
    private static void heapify( SparseBitSet[] work, int len, int i ) {
      int min = i;  // Initialize min as root
      int lf = (i<<1) + 1;
      int rt = (i<<1) + 2;
      // Get min index & swap to heap position
      if( lf < len && e2int(work,lf) < e2int(work,min) ) min = lf;
      if( rt < len && e2int(work,rt) < e2int(work,min) ) min = rt;
      if( min != i ) {           // If min is not root
        swap(work,i,min);        // Swap both array elements
        heapify(work, len, min); // Recursively heapify the affected sub-tree
      }      
    }
    private static long e2int( SparseBitSet[] work, int idx ) { return work[idx].fast_cardinality(); }
    private static void swap( SparseBitSet[] work, int i, int j ) {
      SparseBitSet tmp = work[i]; work[i] = work[j]; work[j] = tmp;
    }
  }
  
  // -------------------------------
  // Best Run So Far, for SF100.
  //
  // Query plan#3
  // pass1: Count tags
  // pass2: ForAll P2's; sum += #P1.tags*#P3.tags
  // err3 : ForAll P1's; sum += #P1P2*#P1.tags
  // err4 : ForAll P1's,
  //          ForAll P1.P2's,
  //            if( P1.has(P1.P2.P3) ) sum += tags
  // Result is pass2-err3-err4
  @Override public long run() {
    long t0 = System.currentTimeMillis(), t;
    
    // Person #Tags
    // Restructure to hash-of-hashes, or hash-of-(sparse_bit_set).
    Vec vid = TSMB.PERSON_HASINTEREST_TAG.vec("id");
    Vec vtg = TSMB.PERSON_HASINTEREST_TAG.vec("hasinterest_tag");
    NonBlockingHashMapLong<Long> ptags = new BuildsTags2().doAll(vid,vtg)._tags;
    if( PRINT_TIMING ) { t=System.currentTimeMillis(); System.out.println("Restructure P1P2#"+ptags.size()+" "+(t-t0)+" msec"); t0=t; }

    // The next 3 passes can run in parallel
    
    // pass2: ForAll P2's; sum += #P1.tags*#P3.tags
    // This over-counts where p1==p3 or not(PKP(p1,p3)), but does not require a
    // loop over the PKP relation
    long cnt = new AllTags(ptags).doAll(TSMB.PERSON.vec("id"))._cnt;
    if( PRINT_TIMING ) { t=System.currentTimeMillis(); System.out.println("AllTags# "+cnt+" "+(t-t0)+" msec"); t0=t; }
    
    // err3 : ForAll P1's; sum += #P1P2*#P1.tags
    // Correct for p1==p3.
    long err3 = new Err3(ptags).doAll(TSMB.PERSON.vec("id"))._cnt;
    if( PRINT_TIMING ) { t=System.currentTimeMillis(); System.out.println("Err3# "+err3+" "+(t-t0)+" msec"); t0=t; }
    
    // err4 : ForAll P1's,
    // Correct for not(PKP(p1,p3))
    long err4 = new Err4(ptags).doAll(TSMB.PERSON.vec("id"))._cnt;
    if( PRINT_TIMING ) { t=System.currentTimeMillis(); System.out.println("Err4# "+err4+" "+(t-t0)+" msec"); t0=t; }
    
    return cnt - err3 - err4;
  }

  // pass2: ForAll P2's; sum += #P1.tags*#P3.tags
  // This over-counts where p1==p3 or not(PKP(p1,p3)), but does not require a loop over PKP
  //   51009398 // actual
  private static class AllTags extends MRTask<AllTags> {
    long _cnt;
    final NonBlockingHashMapLong<Long> _ptags;
    AllTags( NonBlockingHashMapLong<Long> ptags ) { _ptags=ptags; }
    @Override public void map( Chunk pids ) {
      long cnt=0;
      for( int i=0; i<pids._len; i++ ) {
        long p2 = pids.at8(i);
        SparseBitSet p13s = TSMB.P_KNOWS_P.get(p2); // Set of p2->p1 and p2->p3
        if( p13s != null ) {
          long sum=0;
          for( long p13 : p13s.rawKeySet() )
            if( p13!=0 )
              sum += _ptags.get(p13);
          cnt += sum*p13s.fast_cardinality();
        }
      }
      _cnt=cnt;
    }
    @Override public void reduce( AllTags bld ) { _cnt += bld._cnt; }
  }

  // err3 : ForAll P1's; sum += #P1P2*#P1.tags
  // Correct for p1==p3.
  //    839613 // observed correction
  private static class Err3 extends MRTask<Err3> {
    long _cnt;
    final NonBlockingHashMapLong<Long> _ptags;
    Err3( NonBlockingHashMapLong<Long>  ptags ) { _ptags=ptags; }
    @Override public void map( Chunk pids ) {
      long cnt=0;
      for( int i=0; i<pids._len; i++ ) {
        long p1 = pids.at8(i);
        SparseBitSet p12s = TSMB.P_KNOWS_P.get(p1); // Set of p1->p2
        if( p12s != null )
          cnt += _ptags.get(p1)*p12s.fast_cardinality();
      }
      _cnt=cnt;
    }
    @Override public void reduce( Err3 bld ) { _cnt += bld._cnt; }
  }

  // err4 : ForAll P1's,
  //          Put P2s on sorted worklist, small to large
  //            ForAll P1.P2's,
  //              if( P1.has(P1.P2.P3) ) sum += #P1.tags
  // Correct for not(PKP(p1,p3))
  private static class Err4 extends MRTask<Err4> {
    long _cnt;
    final NonBlockingHashMapLong<Long> _ptags;
    Err4( NonBlockingHashMapLong<Long>  ptags ) { _ptags=ptags; }
    @Override public void map( Chunk pids ) {
      long cnt=0;
      for( int i=0; i<pids._len; i++ ) {
        long p1 = pids.at8(i);
        SparseBitSet p2s = TSMB.P_KNOWS_P.get(p1); // Set of p2->p13s
        if( p2s != null ) {
          long np1cnt = _ptags.get(p1);
          for( long p2 : p2s.rawKeySet() ) // ForAll P2s, do P1
            if( p2 != 0 ) {
              SparseBitSet p3s = TSMB.P_KNOWS_P.get(p2); // Set of p2->p3s
              for( long p3 : p3s.rawKeySet() )
                if( p3 != 0 && p2s.tst(p3) ) // does p1 know p3?
                  cnt += np1cnt;
            }
        }
      }
      _cnt=cnt;
    }
    @Override public void reduce( Err4 bld ) { _cnt += bld._cnt; }
  }

  // -------------------------------
  // Query plan#2:
  //   Same as plan#1, but count tags only, assuming no dup tags
  
  // Compute hash of Persons with interest tags
  // ForAll P1s
  //   ForAll P1.P2s (and the inverted case)
  //     ForAll P2.P3s
  //       if( P1!=P3 && !P1.hash(P3) )
  //         count += P3.#interest_tags;  
  public long run2() {
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
