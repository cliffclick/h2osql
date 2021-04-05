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

            Answer    H2O 20CPU   DOVE1
SF0.1:      200280    0.000 sec   5.350 sec
SF1  :     3107478    0.015 sec
SF10 :    37853736    0.283 sec
SF100:   487437702    4.365 sec
                      3.930 sec using 32bit person ids
*/

/*
Implementation of Dovetail Join by Todd Veldhuizen.

This version is modifed from the base version in Dove0 via:
1-  removing the pad variables
H2O brute force solution times is given above; it is about X faster.
See Dove2 for an improved version.
 */

public class Dove1 implements TSMB.TSMBI {
  @Override public String name() { return "Dove1"; }
  static final boolean PRINT_TIMING = true;

  // -----------------------------------------------------------------
  // Do triangles via "worse case optimal join" or "dove-tail join".
  // Sort the edge array.  Use an iterator to walk it.  Implement
  // a "seek least upper bound"
  public long run() {
    long t0 = System.currentTimeMillis(), t;

    // Make directed edges undirected, by doubling the edge entries.
    Frame pids = TSMB.PERSON_KNOWS_PERSON.subframe(new String[]{"dp1","dp2"});
    Frame dids = new Undirect().doAll(pids.types(),pids).outputFrame(pids._names,null);
    if( PRINT_TIMING ) { t=System.currentTimeMillis(); System.out.println("Double edges#"+dids.numRows()+" "+(t-t0)+" msec"); t0=t; }
    
    // Sort
    Frame sids = Merge.sort(dids,new int[]{0,1});
    dids.delete();
    if( PRINT_TIMING ) { t=System.currentTimeMillis(); System.out.println("Sort#"+sids.numRows()+" "+(t-t0)+" msec"); t0=t; }

    // Dovetail join, counting hits
    long cnt = join_triangles(sids.vec(0),sids.vec(1));
    sids.delete();
    if( PRINT_TIMING ) { t=System.currentTimeMillis(); System.out.println("Dovetail "+(t-t0)+" msec"); t0=t; }

    return cnt;
  }

  // Make directed graph edges, undirected by duplication
  private static class Undirect extends MRTask<Undirect> {
    @Override public void map( Chunk cs[], NewChunk ncs[] ) {
      for( int i=0; i<cs[0]._len; i++ ) {
        ncs[0].addNum(cs[0].at8(i));
        ncs[1].addNum(cs[1].at8(i));
        ncs[0].addNum(cs[1].at8(i));
        ncs[1].addNum(cs[0].at8(i));
      }
    }
  }

  // Probably belongs in ArrayUtils.
  public static int cmp( int[] keys0, int[] keys1 ) {
    for( int i=0; i<keys0.length; i++ )
      if( keys0[i]-keys1[i] != 0 ) return keys0[i]-keys1[i];
    return 0;
  }

  // Silly iterator class to make it easier to do a first implementation.
  private abstract static class Iter implements Comparable<Iter> {
    final static int PINF = Integer.MAX_VALUE;
    final static int NINF = -1; // compare of any 2 keys uses subtract, which cannot wrap.
    int _padx;                  // Which of the 3 keys is padding
    // The 2-D relation being walked over.  The relation is of the form (X<->Y)
    // and is fairly sparse.  The encoding is a sorted list of (X,Y) pairs.
    final Vec.Reader vx,vy;     // Underlying bits being iterated over
    final int _nrows;           // Fast/local number of encoded rows, or set-bits in the relation
    int _pos;                   // Position of dense relation matching the padded keys
    final int[] _keys;          // Keys of the padded iterator.  The max of these is shared as the join point.
    Iter(Vec vec0, Vec vec1) {
      _nrows = (int)vec0.length();
      vx = vec0.new Reader();
      vy = vec1.new Reader();
      _pos = -1;
      _keys = new int[3];  Arrays.fill(_keys,NINF);
      init();
    }

    // Compare two (padded) iterators lexicographically.
    @Override public int compareTo( Iter iter ) { return cmp(_keys,iter._keys); }
    abstract void init();
    final boolean at_end() { return _pos >= _nrows; }
    
    // Seek Least-Upper-Bound of iter n.  Adjusts elements of 'es' to match the
    // iter that moves.
    
    final int[] seek_lub(int[] es) {
      int e0 = _padx==0 ? es[1] : es[0];
      int e1 = _padx==2 ? es[1] : es[2];
      // TODO: There's an efficiency hack, where i start from current pos instead
      // of from top-down.
      int pos = binsearch(e0,e1);
      if( pos==_pos ) {
        _keys[_padx] = es[_padx]; // Only move pad
        return cmp(_keys,es) > 0 ? _keys : es;
      } 
      int x = pos < _nrows ? vx.at4(pos) : PINF;
      int y = pos < _nrows ? vy.at4(pos) : PINF;
      int k0= -1, k1= -1, k2= -1;

      switch( _padx ) {
      case 2:
        // set key0,key1.  If either moves, reset key2
        k0 = x;
        k1 = y;
        k2 = (pos < _nrows && !(k0==es[0] && k1==es[1])) ? NINF : es[2];
        break;
        
      case 1:
        // If key0 moves, instead advance the right-most pad.
        k0 = x;
        k1 = es[1];
        k2 = y;
        if( k0!=es[0] ) { // key0 moves
          pos = binsearch(es[0],NINF);
          assert pos < _nrows;
          k0 = vx.at4(pos);     // Use original position
          k1++;                 // Advance right-most pad
          k2 = vy.at4(pos);
        }
        break;
        
      case 0:
        // Keep pad key0 before any moving key; set key1,key2
        k0 = es[0];
        k1 = x;
        k2 = y;
        break;
      }
      move(k0,k1,k2);
      _pos = pos;
      assert cmp(_keys,es) >= 0;
      return _keys;
    }

    // Top-down find LUB...
    final int binsearch( int key0, int key1 ) {
      int lb = 0, ub = _nrows;
      while( lb < ub ) {
        int mid = lb + ((ub - lb) >> 1);
        int elem0 = vx.at4(mid);
        int elem1 = vy.at4(mid);
        if( elem0==key0 && elem1==key1 ) return mid;
        if( elem0 < key0 || (elem0==key0 && elem1 < key1) ) lb = mid+1;
        else ub = mid;
      }
      return ub; // -ub-1; Can flag the miss, if desired
    }

    final void next() {
      if( _padx==0 ) {
        _pos++;
        if( at_end() ) move(_keys[0],PINF        ,PINF        );
        else           move(_keys[0],vx.at4(_pos),vy.at4(_pos));
      } else {
        assert _padx==2;
        _pos++;
        if( at_end() ) move(PINF        ,PINF,        _keys[2]);
        else           move(vx.at4(_pos),vy.at4(_pos),_keys[2]);
      }
    }
        
    private void move(int e0, int e1, int e2) {
      assert _keys[0] < e0 ||
        ( _keys[0]==e0 &&
          ( _keys[1] < e1 ||
            ( _keys[1]==e1 &&
              ( _keys[2] <= e2 )))) : "monotonic forward";
      _keys[0] = e0;
      _keys[1] = e1;
      _keys[2] = e2;
    }
    
    @Override public String toString() {
      SB sb = new SB().p(_pos).p("#[");
      for( int i=0; i<_keys.length; i++ ) {
        sb.p(' ');
        if     ( _keys[i] == NINF ) sb.p("-inf");
        else if( _keys[i] == PINF ) sb.p("+inf");
        else                        sb.p(_keys[i]);
        if( i==_padx ) sb.p('!');
        sb.p(',');
      }
      return sb.unchar().p(" ]").toString();
    }
  }
  private static class IterR extends Iter { IterR(Vec v0, Vec v1) { super(v0,v1); } void init( ) { _padx = 2; } }
  private static class IterS extends Iter { IterS(Vec v0, Vec v1) { super(v0,v1); } void init( ) { _padx = 1; } }
  private static class IterT extends Iter { IterT(Vec v0, Vec v1) { super(v0,v1); } void init( ) { _padx = 0; } }

  
  // Custom iterator for TSMB11.
  // 
  private long join_triangles( Vec v0, Vec v1 ) {
    // Make an iter for P1->P2 and P2->P3 relations.  Set them to zero rows.
    // All iters are using the same relations.
    Iter iter_r = new IterR(v0,v1);
    Iter iter_s = new IterS(v0,v1);
    Iter iter_t = new IterT(v0,v1);
    Iter[] iters = new Iter[]{iter_r,iter_s,iter_t};

    // Original join point, just uses the zero element.
    int e0 = (int)v0.at8(0);
    int e1 = (int)v0.at8(1);
    int[] es0 = new int[]{e0,e1, Iter.NINF}, es = es0;

    // Until at_end, find first minimal iter, and seek_lub.
    int debug_cnt=0;
    long cnt=0;
    while( !at_end(iters) ) {
      // Find minimal iter
      int iter_min = min_iter(iters);
      // Seek Least-Upper-Bound of iter n.  Adjusts elements of join point 'es'
      // to match the iter that moves.
      es = iters[iter_min].seek_lub(es);
        
      // Are all iters at the join position?
      if( at_pos(iters,es) ) {
        cnt++;                  // Found a join element; do join work
        iters[2].next();        // bump innermost iter
        assert cmp(iters[2]._keys,es) == 0;
      }
      assert cmp(iter_r._keys,es) <= 0;
      assert cmp(iter_s._keys,es) <= 0;
      assert cmp(iter_t._keys,es) <= 0;
      debug_cnt++;
    }

    return cnt;
  }

  // Find the least iterator from a set of iterators
  private static int min_iter( Iter[] iters ) {
    int min=0;
    for( int i=1; i<iters.length; i++ ) {
      int cmp = iters[min].compareTo(iters[i]);
      if( cmp > 0 ) min=i;
    }
    return min;
  }
    
  // At-position; all iters agree, and at least one did a seek_lub previously to agree.
  private static boolean at_pos( Iter[] iters, int[] es ) {
    for( int i=1; i<iters.length; i++ )
      if( iters[0].compareTo(iters[i])!=0 )
        return false;
    return true;
  }

  private static boolean at_end(Iter[] iters) {
    for( Iter iter : iters )
      if( !iter.at_end() )
        return false;
    return true;
  }
}
