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

            Answer    H2O 20CPU   DOVE6
SF0.1:      200280    0.000 sec   0.057  sec
SF1  :     3107478    0.015 sec   0.493  sec
SF10 :    37853736    0.283 sec   7.140  sec
SF100:   487437702    4.365 sec 115.000  sec
                      3.930 sec using 32bit person ids
*/

/*
Implementation of Dovetail Join by Todd Veldhuizen.

This version is modified from the base version in Dove0 via:
1-  removing the pad variables
2-  manually inline padded iter ops
3-  statically track which iters are at-position
4-  statically track which positions are at -inf - NOT DONE (because its super cheap here)
5-  smarter iterator to advance; NO CHANGE: outer iter moves faster
6-  'next' iters after a hit; NO CHANGE: very dataset dependent
6-  manually inline to remove join & iter objects; SLIGHTY WORSE: probably i-cache issues
6-  Parallelize, H2O-style; 10x speedup on 20 cores
H2O brute force solution times is given above; it is about 25X faster.
 */

public class Dove6 implements TSMB.TSMBI {
  @Override public String name() { return "Dove6"; }
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
    //long cnt = join_triangles(sids.vec(0),sids.vec(1));
    long cnt = new Triangles().doAll(sids.vec(0),sids.vec(1))._cnt;
    sids.delete();
    if( PRINT_TIMING ) { t=System.currentTimeMillis(); System.out.println("Dovetail "+(t-t0)+" msec"); t0=t; }

    assert cnt==200280;
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
    // The 2-D relation being walked over.  The relation is of the form (X<->Y)
    // and is fairly sparse.  The encoding is a sorted list of (X,Y) pairs.
    final Vec.Reader _vx,_vy;   // Underlying bits being iterated over
    final int _nrows;           // Fast/local number of encoded rows, or set-bits in the relation
    int _pos;
    int _kx, _ky;               // Key x,y
    Iter(Vec vec0, Vec vec1) {
      _nrows = (int)vec0.length();
      _vx = vec0.new Reader();
      _vy = vec1.new Reader();
      _pos= 0;
      _kx = NINF;
      _ky = NINF;
    }

    // Compare against join point
    abstract int cmp( int[] es );
  
    boolean check_pos(int[]es, boolean before) {
      int tmp=cmp(es);
      return before ? tmp < 0 : tmp == 0;
    }
    
    void next() {
      if( _pos+1 >= _nrows ) return; // Already off end
      _pos++;
      if( _pos==_nrows ) {
        _kx = _ky = PINF;
      } else {
        _kx = _vx.at4(_pos);
        _ky = _vy.at4(_pos);
      }
    }

    // Seek a few nearby positions before falling back to binary search to the LUB.
    final void seek( int key0, int key1 ) {
      // Always seeking forwards
      assert _pos==0 || _pos==_nrows || // At end, OR
        _vx.at4(_pos) < key0 || // before (key0,key1).  
        (_vx.at4(_pos) == key0 && _vy.at(_pos)<=key1 );
      // Try a few nearby positions
      for( int i=0; i<24 && _pos+i<_nrows; i++ ) {
        int kx = _vx.at4(_pos+i);
        int ky = _vy.at4(_pos+i);
        if( kx>key0 || (kx==key0 && ky>=key1) ) {
          _kx=kx; _ky=ky;
          _pos = _pos + i;
          return; // Found LUB
        }
      }
      // Tried linear scan, didn't work, use binary search
      _pos = binsearch(key0, key1);
    }

    // Top-down find LUB.
    final int binsearch( int key0, int key1 ) {
      int lb = 0, ub = _nrows;
      while( lb < ub ) {
        int mid = lb + ((ub - lb) >> 1);
        int elem0 = _vx.at4(mid);
        int elem1 = _vy.at4(mid);
        if( elem0==key0 && elem1==key1 ) {
          _kx = elem0;
          _ky = elem1;
          return mid;
        }
        if( elem0 < key0 || (elem0==key0 && elem1 < key1) ) lb = mid+1;
        else ub = mid;
      }
      _kx = ub < _nrows ? _vx.at4(ub) : PINF;
      _ky = ub < _nrows ? _vy.at4(ub) : PINF;
      return ub; // -ub-1; Can flag the miss, if desired
    }

    static SB str(SB sb, int key) {
      if( key == NINF ) return sb.p("-inf");
      if( key == PINF ) return sb.p("+inf");
      return sb.p(key);
    }
  }
  private static class IterR extends Iter {
    IterR(Vec v0, Vec v1) { super(v0,v1); }
    // Compare against the join point
    @Override int cmp( int[] es ) {
      int dx = _kx-es[0];
      return dx==0 ? _ky-es[1] : dx;
    }
    @Override public String toString() {
      SB sb = new SB().p("[ ");
      str(sb,_kx).p(", ");
      str(sb,_ky).p(", _");
      return sb.p(" ]").toString();
    }
  }
  private static class IterS extends Iter {
    IterS(Vec v0, Vec v1) { super(v0,v1); }
    // Compare against the join point
    @Override int cmp( int[] es ) {
      int dx = _kx-es[0];
      return dx==0 ? _ky-es[2] : dx;
    }
    @Override public String toString() {
      SB sb = new SB().p("[ ");
      str(sb,_kx).p(", _,");
      str(sb,_ky);
      return sb.p(" ]").toString();
    }
  }
  private static class IterT extends Iter {
    IterT(Vec v0, Vec v1) { super(v0,v1); }
    // Compare against the join point
    @Override int cmp( int[] es ) {
      int dx = _kx-es[1];
      return dx==0 ? _ky-es[2] : dx;
    }
    @Override public String toString() {
      SB sb = new SB().p("[ _, ");
      str(sb,_kx).p(", ");
      str(sb,_ky);
      return sb.p(" ]").toString();
    }
  }

  
  // Custom iterator for TSMB11.
  // 
  private long join_triangles( Vec v0, Vec v1 ) {
    // Make an iter for P1->P2 and P2->P3 relations.  Set them to zero rows.
    // All iters are using the same relations.
    final IterR iter_r = new IterR(v0,v1);
    final IterS iter_s = new IterS(v0,v1);
    final IterT iter_t = new IterT(v0,v1);

    // Original join point, just uses the zero element.
    final int e0 = (int)v0.at8(0);
    final int e1 = (int)v1.at8(0);
    final int[] es = new int[]{e0,e1, e1};

    // Until at_end, find first minimal iter, and seek_lub.
    long cnt=0;                 // The answer
    int state=0;                // which iters are at-position
    while( !at_end(es) ) {

      if(      (state&1)==0 )  state = seek_R(iter_r,iter_s,iter_t,es,state);
      else if( (state&2)==0 )  state = seek_S(iter_r,iter_s,iter_t,es,state);
      else if( (state&4)==0 )  state = seek_T(iter_r,iter_s,iter_t,es,state);
      assert iter_r.check_pos(es,(state&1)==0);
      assert iter_s.check_pos(es,(state&2)==0);
      assert iter_t.check_pos(es,(state&4)==0);
      
      // Are all iters at the join position?
      if( state==7 ) {
        cnt++;                  // Found a join element; do join work
        es[es.length-1]++;      // Bump the innermost join point
        state=1;                // R keeps at-pos, but S & T do not
      }
    }

    return cnt;
  }

  private static int seek_R(IterR iter_r, IterS iter_s, IterT iter_t, int[] es, int state) {
    // Seek LUB for iterR, seek PAD for iterS,T
    iter_r.seek(es[0],es[1]);
    if( es[0]!=iter_r._kx ) {
      es[0] = iter_r._kx;       // Moving es[0] might blow iter_s
      iter_t._kx = iter_t._ky = es[2] = Iter.NINF; // iter_t is defnitely blown
      iter_t._pos=0;
      state&=3;                                    // clear iter_t
    }
    if( es[1]!=iter_r._ky ) { // Reset after pad, iter_s not-at-pos
      es[1] = iter_r._ky;     // Moving es[1] might blow iter_t
      iter_s._ky = es[2] = Iter.NINF;
      iter_s._pos=0;
      state = es[0]==iter_s._kx ? 2 : 0;
    } 
    return state|1;
  }
  private static int seek_S(IterR iter_r, IterS iter_s, IterT iter_t, int[] es, int state) {
    // Seek LUB for iterS, seek PAD for iterR,T
    iter_s.seek(es[0],es[2]);
    // If key0 moves, bump right-most trailing pad by 1 & use NINF for remaining keys
    if( iter_s._kx!=es[0] ) {        // key0 moves
      if( es[1]!=Iter.PINF ) {
        iter_s._pos = iter_s.binsearch(es[0], Iter.NINF);
        es[1]++;     // Advance pad just left of right-most reset key
        state = 0;   // iter-T and iter-R is not at-pos
      } else {       // Cannot bump pad key, so keep the iter_s bump and reset pad
        es[0] = iter_s._kx;
        es[1] = Iter.NINF;
        iter_t._kx = iter_t._ky = Iter.NINF;
        state = 0;   // iter-T and iter-R is not at-pos
      }
    } else {
      if( iter_t._kx!=es[1] || iter_t._ky!=iter_s._ky )  state &= 3; // iter-T is not-at-pos.
    }
    es[2] = iter_s._ky;
    // iter-R is unchanged
    // iter-S is at-pos
    return state|2;
  }
  private static int seek_T(IterR iter_r, IterS iter_s, IterT iter_t, int[] es, int state) {
    // Seek LUB for iterT, seek PAD for iterR,S
    iter_t.seek(es[1],es[2]);
    if( es[1]!=iter_t._kx ) { iter_s._ky = Iter.NINF; iter_s._pos=0; state&=4; } // Reset after pad; moves es[1] so blows R,S
    // Keep pad key0 before any moving key; set key1,key2
    es[1] = iter_t._kx; 
    if( es[2]!=iter_t._ky ) state&=5; // blows S
    es[2] = iter_t._ky;
    return state|4;
  }

  private static boolean at_end( int[] es ) { return es[0]== Iter.PINF; }


  // --------------------------------------------------------------------------
  private static class Triangles extends MRTask<Triangles> {
    long _cnt;
    @Override public void map( Chunk c0, Chunk c1 ) {
      long cnt=0;
      Vec v0 = c0.vec(), v1 = c1.vec();
      long start = c0.start(), end = start+c0._len, nrows = v0.length();

      // Make an iter for P1->P2 and P2->P3 relations.
      final IterR iter_r = new IterR(v0,v1);
      final IterS iter_s = new IterS(v0,v1);
      final IterT iter_t = new IterT(v0,v1);
      
      // Initial & ending join point (inclusive,exclusive):
      int e0 = end < nrows ? (int)v0.at8(end) : Iter.PINF;
      int e1 = end < nrows ? (int)v1.at8(end) : Iter.PINF;
      final int[] es  = new int[]{(int)v0.at8(start),(int)v1.at8(start),Iter.NINF};
      final int[] last= new int[]{     e0,                e1,           Iter.NINF};
      
      // Until at_end, find first minimal iter, and seek_lub.
      int state=0;                // which iters are at-position      
      while( cmp(es,last) < 0 ) {

        if(      (state&1)==0 )  state = seek_R(iter_r,iter_s,iter_t,es,state);
        else if( (state&2)==0 )  state = seek_S(iter_r,iter_s,iter_t,es,state);
        else if( (state&4)==0 )  state = seek_T(iter_r,iter_s,iter_t,es,state);
        assert iter_r.check_pos(es,(state&1)==0);
        assert iter_s.check_pos(es,(state&2)==0);
        assert iter_t.check_pos(es,(state&4)==0);
        
        // Are all iters at the join position?
        if( state==7 ) {
          cnt++;                  // Found a join element; do join work
          es[es.length-1]++;      // Bump the innermost join point
          state=1;                // R keeps at-pos, but S & T do not
        }
      }
      
      _cnt=cnt;
    }
    @Override public void reduce( Triangles C ) { _cnt += C._cnt; }
  }

}
