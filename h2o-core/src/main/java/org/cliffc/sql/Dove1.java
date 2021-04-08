package org.cliffc.sql;

import water.*;
import water.fvec.*;
import water.rapids.Merge;
import water.util.SB;

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
  static final boolean PRINT_TIMING = false;

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

    assert cnt==200280; // actually only valid for SF0.1
    return cnt;
  }

  // Make directed graph edges, undirected by duplication
  private static class Undirect extends MRTask<Undirect> {
    @Override public void map( Chunk[] cs, NewChunk[] ncs ) {
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
  private abstract static class Iter {
    final static int PINF = Integer.MAX_VALUE-1;
    final static int NINF = -1; // compare of any 2 keys uses subtract, which cannot wrap.
    final int _ix, _iy;         // Index of the keys in the padded-key layout
    // The 2-D relation being walked over.  The relation is of the form (X<->Y)
    // and is fairly sparse.  The encoding is a sorted list of (X,Y) pairs.
    final Vec.Reader _vx,_vy;   // Underlying bits being iterated over
    final int _nrows;           // Fast/local number of encoded rows, or set-bits in the relation
    int _pos;                   // Position of dense relation matching the padded keys
    int _kx, _ky;               // Key x,y
    Iter(Vec vec0, Vec vec1) {
      _nrows = (int)vec0.length();
      _vx = vec0.new Reader();
      _vy = vec1.new Reader();
      _ix = ix();
      _iy = iy();
      _pos = 0;
      _kx = _vx.at4(_pos);
      _ky = _vy.at4(_pos);
    }

    // Return key[i]
    int keyi( int[] es, int i ) {
      if( i==_ix ) return _kx;
      if( i==_iy ) return _ky;
      return es[i];
    }

    // Compare two (padded) iterators lexicographically.
    public int cmp( int[] es, Iter iter ) {
      for( int i=0; i<es.length; i++ ) {
        int d = keyi(es,i)-iter.keyi(es,i);
        if( d!=0 ) return d;
      }
      return 0;
    }
    // Compare against the join point
    public int cmp( int[] es ) {
      int dx = _kx-es[_ix];
      return dx!=0 ? dx : _ky-es[_iy];
    }
    abstract int ix();
    abstract int iy();
    abstract void seek_lub( int[] es );
    abstract void seek_pad( int[] prev, int[] es );
    final boolean at_end() { return _pos >= _nrows; }

    // Top-down find LUB.
    // TODO: There's an efficiency hack, where i start from current pos instead
    // of from top-down.
    final int binsearch( int key0, int key1 ) {
      int lb = 0, ub = _nrows;
      while( lb < ub ) {
        int mid = lb + ((ub - lb) >> 1);
        int elem0 = _vx.at4(mid);
        int elem1 = _vy.at4(mid);
        if( elem0==key0 && elem1==key1 ) return mid;
        if( elem0 < key0 || (elem0==key0 && elem1 < key1) ) lb = mid+1;
        else ub = mid;
      }
      return ub; // -ub-1; Can flag the miss, if desired
    }

    @Override public String toString() {
      SB sb = new SB().p(_pos).p("#[");
      for( int i=0; i<3; i++ ) {
        sb.p(' ');
        if( i== _ix ) str(sb,_kx);
        else if( i== _iy ) str(sb,_ky);
        else sb.p('_');
        sb.p(',');
      }
      return sb.unchar().p(" ]").toString();
    }
    private static SB str(SB sb, int key) {
      if( key == NINF ) return sb.p("-inf");
      if( key == PINF ) return sb.p("+inf");
      return sb.p(key);
    }
  }
  private static class IterR extends Iter {
    IterR(Vec v0, Vec v1) { super(v0,v1); }
    @Override int ix( ) { return 0; }
    @Override int iy( ) { return 1; }
    @Override void seek_pad( int[] prev, int[] es ) { /*empty; pad after, no seek required*/}
    // Seek Least-Upper-Bound of iter n.  Adjusts elements of 'es' to match the
    // iter that moves.
    @Override void seek_lub(int[] es) {
      int pos = binsearch(es[0],es[1]);
      int x = _kx = pos < _nrows ? _vx.at4(pos) : PINF;
      int y = _ky = pos < _nrows ? _vy.at4(pos) : PINF;
      // set key0,key1.  If either moves, reset key2
      if( pos < _nrows && !(x==es[0] && y==es[1]))
        es[2] = NINF;         // moves the pad; need to 'seek' others
      es[0] = x;
      es[1] = y;
      _pos = pos;
      assert cmp(es) == 0;
    }
  }
  private static class IterS extends Iter {
    IterS(Vec v0, Vec v1) { super(v0,v1); }
    @Override int ix( ) { return 0; }
    @Override int iy( ) { return 2; }
    @Override void seek_pad( int[] prev, int[] es ) {
      if( prev[1]==es[1] ) return; // pad did not move, so iter does not move
      _ky = NINF; // Reset after pad
      _pos = -1;  // not at pos
    }
    // Seek Least-Upper-Bound of iter n.  Adjusts elements of 'es' to match the
    // iter that moves.
    @Override void seek_lub(int[] es) {
      int pos = binsearch(es[0],es[2]);
      int x = _kx = pos < _nrows ? _vx.at4(pos) : PINF;
      int y = _ky = pos < _nrows ? _vy.at4(pos) : PINF;
      // If key0 moves, bump right-most trailing pad by 1 & use NINF for remaining keys
      if( x!=es[0] && es[0]!=NINF ) {        // key0 moves
        int pos2 = binsearch(es[0],NINF);
        assert pos2 < _nrows;
        assert es[0]==_vx.at4(pos2);
        _kx = es[0]; // Use original position
        es[1]++;     // Advance pad just left of right-most reset key          
        _ky = y = _vy.at4(pos2); // Reset to min for es[0]
        pos = -1;    // no such valid pos; keeping kx from pos but ky from pos2
      }
      es[2] = y;
      _pos = pos;
      assert cmp(es) == 0;
    }
  }
  private static class IterT extends Iter {
    IterT(Vec v0, Vec v1) { super(v0,v1); }
    @Override int ix( ) { return 1; }
    @Override int iy( ) { return 2; }
    @Override void seek_pad( int[] prev, int[] es ) {
      if( prev[0]==es[0] ) return; // pad did not move, so iter does not move
      _kx = NINF; // Reset after pad
      _ky = NINF; // Reset after pad
      _pos = -1;  // not at pos
    }
    // Seek Least-Upper-Bound of iter n.  Adjusts elements of 'es' to match the
    // iter that moves.
    @Override void seek_lub(int[] es) {
      int pos = binsearch(es[1],es[2]);
      int x = _kx = pos < _nrows ? _vx.at4(pos) : PINF;
      int y = _ky = pos < _nrows ? _vy.at4(pos) : PINF;
      // Keep pad key0 before any moving key; set key1,key2
      es[1] = x;
      es[2] = y;
      _pos = pos;
      assert cmp(es) == 0;
    }
  }

  
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
    int e1 = (int)v1.at8(0);
    int[] es = new int[]{e0,e1, e1};

    // Until at_end, find first minimal iter, and seek_lub.
    int[] prev = new int[3];
    int debug_cnt=0, DEBUG_CNT=-1;
    long cnt=0;
    while( !at_end(es) ) {
      // Find minimal iter
      int iter_min = min_iter(es,iters);

      // Copy the current join point, for the current padding
      System.arraycopy(es,0,prev,0,3);
      
      // Seek Least-Upper-Bound of iter n.  Adjusts elements of join point 'es'
      // to match the iter that moves.
      iters[iter_min].seek_lub(es);

      // Seek-to-pad the remaining iters
      for( Iter iter : iters )
        if( iter!=iters[iter_min] )
          iter.seek_pad(prev,es);
      
      assert iter_r.cmp(es) <= 0;
      assert iter_s.cmp(es) <= 0;
      assert iter_t.cmp(es) <= 0;
        
      // Are all iters at the join position?
      if( at_pos(es,iters) ) {
        cnt++;                  // Found a join element; do join work
        es[es.length-1]++;      // Bump the innermost join point
      }
      debug_cnt++;
    }

    return cnt;
  }

  // Find the least iterator from a set of iterators
  private static int min_iter( int[] es, Iter[] iters ) {
    int min=0;
    for( int i=1; i<iters.length; i++ ) {
      int cmp = iters[min].cmp(es,iters[i]);
      if( cmp > 0 ) min=i;
    }
    return min;
  }
    
  // At-position; all iters at the join position
  private static boolean at_pos( int[] es, Iter[] iters ) {
    for( Iter iter : iters )
      if( iter.cmp(es) != 0 )
        return false;
    return true;
  }

  private static boolean at_end( int[] es ) { return es[0]== Iter.PINF; }
}
