package org.cliffc.sql;

import water.nbhm.ConcurrentAutoTable;
import water.util.SB;

import java.io.IOException;
import java.io.Serializable;
import java.util.*;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import static water.nbhm.UtilUnsafe.UNSAFE;
import static water.nbhm.UtilUnsafe.fieldOffset;


/**
 * A lock-free Sparse Bit Set, with integer (not long) members to keep the size
 * smaller.  Sparse means not-dense; the members can be randomly distributed
 * about the integer range without increasing the SBS size.  Loosely based on
 * NBHML but with integer keys and no values.
 *
 * to allow concurrent table resize, i burn a bit to 'mark' payload.
 * to allow delete requires either another bit (can be resurrected) or
 * a sentinel (just one state).  So...
 *  
 * negative elements are not allowed; words are marked by complementing;
 * elements are deleted with -1, and canNOT be resurrected.
 * Deleted sentinels are only removed on a table copy.
 *
 * @since 1.8
 * @author Cliff Click
 */

public class SparseBitSet {

  private static final int REPROBE_LIMIT=8; // Too many reprobes then force a table-resize

  // --- Bits to allow Unsafe access to arrays
  private static final int _Lbase  = UNSAFE.arrayBaseOffset(long[].class);
  private static final int _Lscale = UNSAFE.arrayIndexScale(long[].class);
  private static long rawIndex(final long[] ary, final int idx) {
    assert idx >= 0 && idx < ary.length;
    // Note the long-math requirement, to handle arrays of more than 2^31 bytes
    // - or 2^28 - or about 268M - 8-byte pointer elements.
    return _Lbase + ((long)idx * _Lscale);
  }

  // --- Bits to allow Unsafe CAS'ing of the CHM field
  private static final long _chm_offset = fieldOffset(SparseBitSet.class, "_chm");
  private final boolean CAS_chm( final long offset, final CHM old, final CHM nnn ) {
    return UNSAFE.compareAndSwapObject(this, offset, old, nnn );
  }

  // --- The Hash Table --------------------
  private transient CHM _chm;
  // This next field holds the value for Key 0 - the special key value which
  // is the initial array value, and also means: no-key-inserted-yet.
  private final transient AtomicInteger _zero; // Value for Key: NO_KEY

  // Time since last resize
  private transient long _last_resize_milli;

  // Optimize for space: use a 1/2-sized table and allow more re-probes
  private final boolean _opt_for_space;

  // --- Minimum table size ----------------
  // Pick size 4 Keys, which turns into 4*8+24 = 56 bytes on 64-bit JVM.
  private static final int MIN_SIZE_LOG=2;             //
  private static final int MIN_SIZE=(1<<MIN_SIZE_LOG); // Must be power of 2

  // --- Sentinels -------------------------
  // I exclude 1 integer from the 2^64 possibilities, and test for it before
  // entering the main array.  The NO_KEY value must be zero, the initial
  // value set by Java before it hands me the array.
  private static final long NO_KEY = 0;
  private static final long TOMBSTONE = Long.MAX_VALUE;

  //// Count of reprobes
  //private transient ConcurrentAutoTable _reprobes = new ConcurrentAutoTable();
  //private transient ConcurrentAutoTable _tsts     = new ConcurrentAutoTable();
  ///** Get and clear the current count of reprobes.  Reprobes happen on key
  // *  collisions, and a high reprobe rate may indicate a poor hash function or
  // *  weaknesses in the table resizing function.
  // *  @return the count of reprobes since the last call to {@link #reprobes}
  // *  or since the table was created.   */
  //public long reprobes() { long r = _reprobes.get(); _reprobes = new ConcurrentAutoTable(); return r; }
  //public long tsts    () { long r = _tsts    .get(); _tsts     = new ConcurrentAutoTable(); return r; }


  // --- reprobe_limit -----------------------------------------------------
  // Heuristic to decide if we have reprobed toooo many times.  Running over
  // the reprobe limit on a 'get' call acts as a 'miss'; on a 'put' call it
  // can trigger a table resize.  Several places must have exact agreement on
  // what the reprobe_limit is, so we share it here.
  private static int reprobe_limit( int len ) {
    return REPROBE_LIMIT + (len>>5);
  }

  // --- SparseBitSet ----------------------------------------------
  // Constructors

  /** Create a new SparseBitSet with default minimum size */
  public SparseBitSet( ) { this(MIN_SIZE,true); }

  /** Create a new SparseBitSet with initial room for the given
   *  number of elements, thus avoiding internal resizing operations to reach
   *  an appropriate size.  Large numbers here when used with a small count of
   *  elements will sacrifice space for a small amount of time gained.  The
   *  initial size will be rounded up internally to the next larger power of 2. */
  public SparseBitSet( final int initial_sz ) { this(initial_sz,true); }

  /** Create a new SparseBitSet, setting the space-for-speed
   *  tradeoff.  {@code true} optimizes for space and is the default.  {@code
   *  false} optimizes for speed and doubles space costs for roughly a 10%
   *  speed improvement.  */
  public SparseBitSet( final boolean opt_for_space ) { this(1,opt_for_space); }

  /** Create a new SparseBitSet, setting both the initial size and
   *  the space-for-speed tradeoff.  {@code true} optimizes for space and is
   *  the default.  {@code false} optimizes for speed and doubles space costs
   *  for roughly a 10% speed improvement.  */
  public SparseBitSet( final int initial_sz, final boolean opt_for_space ) {
    _opt_for_space = opt_for_space;
    _zero = new AtomicInteger();
    initialize(initial_sz);
  }
  private void initialize( final int initial_sz ) {
    if( initial_sz < 0 ) throw new IllegalArgumentException("initial_sz argument must be >= 0");
    int i;                      // Convert to next largest power-of-2
    for( i=MIN_SIZE_LOG; (1<<i) < initial_sz; i++ ) {/*empty*/}
    _chm = new CHM(this,new ConcurrentAutoTable(),i);
    _last_resize_milli = System.currentTimeMillis();
  }

  // --- wrappers ------------------------------------------------------------

  /** Returns an approximatation of the bytesize
   *  @return approximate bytesize */
  public int size( ) { return 5*8 + _chm.size(); }
  /** Returns the count of set bits 
   *  @return the count of set bits */
  public long cardinality( ) { return _zero.get() + _chm.cardinality(); }
  public long fast_cardinality( ) { return _zero.get() + _chm.fast_cardinality(); }

  /** Tests if bit is set.
   * @return <tt>true</tt> if bit is set */
  public boolean tst( long e ) {
    if( e<0 ) throw new IllegalArgumentException("Positive bits only");
    return e==0 ? _zero.get()!=0 : _chm.tst(e);
  }

  /** Sets bit.
   * @return Old value of bit */
  public boolean set( long e ) {
    if( e<0 ) throw new IllegalArgumentException("Positive bits only");
    return e==0 ? _zero.getAndSet(1)!=0 : _chm.set(e,false);
  }

  /** Clears bit.
   * @return Old value of bit */
  public boolean clr( long e ) {
    if( e<0 ) throw new IllegalArgumentException("Positive bits only");
    return e==0 ? _zero.getAndSet(0)!=0 : _chm.clr(e);
  }

  /** Clears all bits */
  public void clear() {         // Smack a new empty table down
    CHM newchm = new CHM(this,new ConcurrentAutoTable(),MIN_SIZE_LOG);
    while( !CAS_chm(_chm_offset,_chm,newchm) ) { /*Spin until the clear works*/}
    _zero.set(0);
  }

  // --- help_copy -----------------------------------------------------------
  // Help along an existing resize operation.  This is just a fast cut-out
  // wrapper, to encourage inlining for the fast no-copy-in-progress case.  We
  // always help the top-most table copy, even if there are nested table
  // copies in progress.
  private void help_copy( ) {
    // Read the top-level CHM only once.  We'll try to help this copy along,
    // even if it gets promoted out from under us (i.e., the copy completes
    // and another KVS becomes the top-level copy).
    CHM topchm = _chm;
    if( topchm._newchm == null ) return; // No copy in-progress
    topchm.help_copy_impl(false);
  }

  // --- hash ----------------------------------------------------------------
  // Helper function to spread lousy hashCodes.
  private static int hash(long h) {
    h ^= (h>>>20) ^ (h>>>12);
    h ^= (h>>> 7) ^ (h>>> 4);
    h += h<<7; // smear low bits up high, for hashcodes that only differ by 1
    return (int)h;
  }

  public long[] rawKeySet() {
    while( true ) {           // Verify no table-copy-in-progress
      CHM topchm = _chm;
      if( topchm._newchm == null ) // No table-copy-in-progress
        return topchm._keys;
      // Table copy in-progress - so we cannot get a clean iteration.  We
      // must help finish the table copy before we can start iterating.
      topchm.help_copy_impl(true);
    }
  }
  
  @Override public String toString() { return str(new SB()).toString(); }
  public SB str(SB sb) {
    sb.p('[');
    if( _zero.get()!=0 ) sb.p("0,");
    for( long key : rawKeySet() )
      if( key>0 )
        sb.p(key).p(',');
    return sb.unchar().p(']');
  }

  // --- CHM -----------------------------------------------------------------
  // The control structure for the SparseBitSet
  private static final class CHM implements Serializable {
    // Back-pointer to top-level structure
    final SparseBitSet _sbs;

    // Keys in the table.  0 is free.  +key is set.  -key is set, but the table
    // is mid-copy and updates need to proceed to the nested table; tests can
    // just return true; Integer.MIN_VALUE is deleted and cannot be reused.
    final long[] _keys;
    // Access Key for a given idx
    private boolean CAS_key( int idx, long old, long key ) {
      return UNSAFE.compareAndSwapLong( _keys, rawIndex(_keys, idx), old, key );
    }

    // Size in active Keys
    private ConcurrentAutoTable _size;
    public int cardinality() { return (int)_size.get(); }
    public int fast_cardinality() { return (int)_size.estimate_get(); }

    // ---
    // New mappings, used during resizing.
    // The 'next' CHM - created during a resize operation.  This represents
    // the new table being copied from the old one.  It's the volatile
    // variable that is read as we cross from one table to the next, to get
    // the required memory orderings.  It monotonically transits from null to
    // set (once).
    volatile CHM _newchm;
    private static final AtomicReferenceFieldUpdater<CHM,CHM> _newchmUpdater =
      AtomicReferenceFieldUpdater.newUpdater(CHM.class,CHM.class, "_newchm");
    // Set the _newchm field if we can.  AtomicUpdaters do not fail spuriously.
    boolean CAS_newchm( CHM newchm ) {
      return _newchmUpdater.compareAndSet(this,null,newchm);
    }
    // Sometimes many threads race to create a new very large table.  Only 1
    // wins the race, but the losers all allocate a junk large table with
    // hefty allocation costs.  Attempt to control the overkill here by
    // throttling attempts to create a new table.  I cannot really block here
    // (lest I lose the non-blocking property) but late-arriving threads can
    // give the initial resizing thread a little time to allocate the initial
    // new table.  The Right Long Term Fix here is to use array-lets and
    // incrementally create the new very large array.  In C I'd make the array
    // with malloc (which would mmap under the hood) which would only eat
    // virtual-address and not real memory - and after Somebody wins then we
    // could in parallel initialize the array.  Java does not allow
    // un-initialized array creation (especially of ref arrays!).
    volatile long _resizers;    // count of threads attempting an initial resize
    private static final AtomicLongFieldUpdater<CHM> _resizerUpdater =
      AtomicLongFieldUpdater.newUpdater(CHM.class, "_resizers");

    // Simple constructor
    CHM( final SparseBitSet sbs, ConcurrentAutoTable size, final int logsize ) {
      _sbs = sbs;
      _size = size;
      _keys = new long[1<<logsize];
    }

    // Estimate of bytesize (mostly the keys array, plus a little for CAT size and other pointers)
    int size() { throw new RuntimeException("unimpl"); }
    
    // --- tst ----------------------------------------------------------
    private boolean tst( final long key ) {
      final int hash = hash(key);
      final int len  = _keys.length;
      int idx = (hash & (len-1)); // First key hash
      //_sbs._tsts.add(1); // reprobe profiling

      // Main spin/reprobe loop, looking for a Key hit
      int reprobe_cnt=0;
      while( true ) {
        final long K = _keys[idx]; // Get key before volatile read, could be NO_KEY
        if( K == NO_KEY ) return false; // A clear miss
        if( K == key ) return true;     // A clear hit
        if( K ==~key ) return true;     // A hit (but copy in progress)
        if( K < 0 ) return copy_slot_and_check(idx,key).tst(key);
        // tst and set must have the same key lookup logic!  But only 'set'
        // needs to force a table-resize for a too-long key-reprobe sequence.
        // Check for too-many-reprobes on get.
        //_sbs._reprobes.add(1); // reprobe profiling
        if( ++reprobe_cnt >= reprobe_limit(len) ) // too many probes
          return _newchm == null // Table copy in progress?
            ? false              // Nope!  A clear miss
            : copy_slot_and_check(idx,key).tst(key); // Retry in the new table

        idx = (idx+1)&(len-1);    // Reprobe by 1!  (could now prefetch)
      }
    }

    // --- set ---------------------------------------------------------
    private boolean set( final long key, boolean is_copy ) {
      final int hash = hash(key);
      final int len = _keys.length;
      int idx = (hash & (len-1)); // The first key

      // Main spin/reprobe loop, looking for a hit or an empty slot.
      int reprobe_cnt=0;
      while( true ) {               // Spin till we get a Key slot
        final long K = _keys[idx];  // Get current key
        if( K == key ) return true; // Already in table
        if( K < 0 )                 // Copy in-progress
          return copy_slot_and_check(idx,key).set(key,is_copy);
        if( K == NO_KEY ) {         // Slot is free?
          // Claim the empty key-slot
          if( CAS_key(idx, K, key) ) { // Claim slot for Key
            if( !is_copy ) _size.add(1); // Raise cardinality
            return false;       // Old was false
          }
          continue;             // CAS to claim the key-slot failed.
        }
        // Miss on wrong key, must reprobe.  get and set must have the same key
        // lookup logic!  Lest 'get' give up looking too soon.
        //_sbs._reprobes.add(1); // reprobe profiling
        if( ++reprobe_cnt >= reprobe_limit(len) ) {
          // We simply must have a new table to do a 'put'.  At this point a
          // 'get' will also go to the new table (if any).  We do not need
          // to claim a key slot (indeed, we cannot find a free one to claim!).
          final CHM newchm = resize();
          if( !is_copy ) _sbs.help_copy(); // help along an existing copy
          return newchm.set(key,is_copy);
        }
        idx = (idx+1)&(len-1); // Reprobe!
      } // End of spinning till we get a Key slot
    }
    
    boolean clr(final long e) { throw new RuntimeException("unimpl"); }

    void histo() {
      int tm=0, ts=0, mk=0, nk=0, ky=0;
      for( long xkey : _keys ) {
        if( xkey==0 ) nk++;
        else if( xkey== TOMBSTONE ) ts++;
        else if( xkey>0 ) ky++;
        else if( xkey==~TOMBSTONE ) tm++;
        else mk++;
      }
      System.out.println("len "+_keys.length+", copyIdx "+_copyIdx+", copyDone "+_copyDone+", ky "+ky+", nk "+nk+", mk "+mk+", ts "+ts+", tm "+tm+" "+this);
    }

    
    // --- resize ------------------------------------------------------------
    // Resizing after too many probes.  "How Big???" heuristics are here.
    // Callers will (not this routine) will 'help_copy' any in-progress copy.
    // Since this routine has a fast cutout for copy-already-started, callers
    // MUST 'help_copy' lest we have a path which forever runs through
    // 'resize' only to discover a copy-in-progress which never progresses.
    private CHM resize() {
      // Check for resize already in progress, probably triggered by another thread
      CHM newchm = _newchm;     // VOLATILE READ
      if( newchm != null )      // See if resize is already in progress
        return newchm;          // Use the new table already

      // No copy in-progress, so start one.  First up: compute new table size.
      int oldlen = _keys.length;// Old count of Keys allowed
      int sz = cardinality();   // Get current table count of active Keys
      int newsz = sz;           // First size estimate

      // Heuristic to determine new size.  We expect plenty of dead-slots-with-keys
      // and we need some decent padding to avoid endless reprobing.
      if( _sbs._opt_for_space ) {
        // This heuristic leads to a much denser table with a higher reprobe rate
        if( sz >= (oldlen>>1) ) // If we are >50% full of keys then...
          newsz = oldlen<<1;    // Double size
      } else {
        if( sz >= (oldlen>>2) ) { // If we are >25% full of keys then...
          newsz = oldlen<<1;      // Double size
          if( sz >= (oldlen>>1) ) // If we are >50% full of keys then...
            newsz = oldlen<<2;    // Double double size
        }
      }

      // Last (re)size operation was very recent?  Then double again
      // despite having few live keys; slows down resize operations
      // for tables subject to a high key churn rate - but do not
      // forever grow the table.  If there is a high key churn rate
      // the table needs a steady state of rare same-size resize
      // operations to clean out the dead keys.
      long tm = System.currentTimeMillis();
      if( newsz <= oldlen &&    // New table would shrink or hold steady?
          tm <= _sbs._last_resize_milli+10000)  // Recent resize (less than 10 sec ago)
        newsz = oldlen<<1;      // Double the existing size

      // Do not shrink, ever.  If we hit this size once, assume we
      // will again.
      if( newsz < oldlen ) newsz = oldlen;

      // Convert to power-of-2
      int log2;
      for( log2=MIN_SIZE_LOG; (1<<log2) < newsz; log2++ ) ; // Compute log2 of size
      long len = ((1L << log2) << 1) + 2;
      // prevent integer overflow - limit of 2^31 elements in a Java array
      // so here, 2^30 + 2 is the largest number of elements in the hash table
      if ((int)len!=len) {
        log2 = 30;
        len = (1L << log2) + 2;
        if (sz > ((len >> 2) + (len >> 1))) throw new RuntimeException("Table is full.");
      }

      // Now limit the number of threads actually allocating memory to a
      // handful - lest we have 750 threads all trying to allocate a giant
      // resized array.
      long r = _resizers;
      while( !_resizerUpdater.compareAndSet(this,r,r+1) )
        r = _resizers;
      // Size calculation: a words (Key) per table entry, plus a handful.  We
      // guess at 64-bit pointers; 32-bit pointers screws up the size calc by
      // 2x but does not screw up the heuristic very much.
      long megs = (((1L<<log2)+8)<<3/*word to bytes*/)>>20/*megs*/;
      if( r >= 2 && megs > 0 ) { // Already 2 guys trying; wait and see
        newchm = _newchm;        // Between dorking around, another thread did it
        if( newchm != null )     // See if resize is already in progress
          return newchm;         // Use the new table already
        // We could use a wait with timeout, so we'll wakeup as soon as the new table
        // is ready, or after the timeout in any case.
        //synchronized( this ) { wait(8*megs); }         // Timeout - we always wakeup
        // For now, sleep a tad and see if the 2 guys already trying to make
        // the table actually get around to making it happen.
        try { Thread.sleep(megs); } catch( Exception e ) { /*empty*/}
      }
      // Last check, since the 'new' below is expensive and there is a chance
      // that another thread slipped in a new thread while we ran the heuristic.
      newchm = _newchm;
      if( newchm != null )      // See if resize is already in progress
        return newchm;          // Use the new table already

      // New CHM - actually allocate the big arrays
      newchm = new CHM(_sbs,_size,log2);

      // Another check after the slow allocation
      if( _newchm != null )     // See if resize is already in progress
        return _newchm;         // Use the new table already

      // The new table must be CAS'd in so only 1 winner amongst duplicate
      // racing resizing threads.  Extra CHM's will be GC'd.
      if( CAS_newchm( newchm ) ) { // NOW a resize-is-in-progress!
        //notifyAll();            // Wake up any sleepers
        //long nano = System.nanoTime();
        //System.out.println(" "+nano+" Resize from "+oldlen+" to "+(1<<log2)+" and had "+(_resizers-1)+" extras" );
        //System.out.print("["+log2);
      } else                    // CAS failed?
        newchm = _newchm;       // Reread new table
      return newchm;
    }


    // The next part of the table to copy.  It monotonically transits from zero
    // to _keys.length.  Visitors to the table can claim 'work chunks' by
    // CAS'ing this field up, then copying the indicated indices from the old
    // table to the new table.  Workers are not required to finish any chunk;
    // the counter simply wraps and work is copied duplicately until somebody
    // somewhere completes the count.
    volatile long _copyIdx = 0;
    static private final AtomicLongFieldUpdater<CHM> _copyIdxUpdater =
      AtomicLongFieldUpdater.newUpdater(CHM.class, "_copyIdx");

    // Work-done reporting.  Used to efficiently signal when we can move to
    // the new table.  From 0 to len(oldkvs) refers to copying from the old
    // table to the new.
    volatile long _copyDone= 0;
    static private final AtomicLongFieldUpdater<CHM> _copyDoneUpdater =
      AtomicLongFieldUpdater.newUpdater(CHM.class, "_copyDone");

    // --- help_copy_impl ----------------------------------------------------
    // Help along an existing resize operation.  We hope its the top-level
    // copy (it was when we started) but this CHM might have been promoted out
    // of the top position.
    private void help_copy_impl( final boolean copy_all ) {
      final CHM newchm = _newchm;
      assert newchm != null;    // Already checked by caller
      int oldlen = _keys.length; // Total amount to copy
      final int MIN_COPY_WORK = Math.min(oldlen,1024); // Limit per-thread work

      // ---
      int panic_start = -1;
      int copyidx=-9999;            // Fool javac to think it's initialized
      while( _copyDone < oldlen ) { // Still needing to copy?
        // Carve out a chunk of work.  The counter wraps around so every
        // thread eventually tries to copy every slot repeatedly.

        // We "panic" if we have tried TWICE to copy every slot - and it still
        // has not happened.  i.e., twice some thread somewhere claimed they
        // would copy 'slot X' (by bumping _copyIdx) but they never claimed to
        // have finished (by bumping _copyDone).  Our choices become limited:
        // we can wait for the work-claimers to finish (and become a blocking
        // algorithm) or do the copy work ourselves.  Tiny tables with huge
        // thread counts trying to copy the table often 'panic'.
        if( panic_start == -1 ) { // No panic?
          copyidx = (int)_copyIdx;
          while( copyidx < (oldlen<<1) && // 'panic' check
                 !_copyIdxUpdater.compareAndSet(this,copyidx,copyidx+MIN_COPY_WORK) )
            copyidx = (int)_copyIdx;     // Re-read
          if( !(copyidx < (oldlen<<1)) ) // Panic!
            panic_start = copyidx;       // Record where we started to panic-copy
        }

        // We now know what to copy.  Try to copy.
        int workdone = 0;
        for( int i=0; i<MIN_COPY_WORK; i++ )
          if( copy_slot((copyidx+i)&(oldlen-1)) ) // Made an oldtable slot go dead?
            workdone++;         // Yes!
        if( workdone > 0 )      // Report work-done occasionally
          copy_check_and_promote( workdone );// See if we can promote
        //for( int i=0; i<MIN_COPY_WORK; i++ )
        //  if( copy_slot((copyidx+i)&(oldlen-1)) ) // Made an oldtable slot go dead?
        //    copy_check_and_promote( 1 );// See if we can promote

        copyidx += MIN_COPY_WORK;
        // Uncomment these next 2 lines to turn on incremental table-copy.
        // Otherwise this thread continues to copy until it is all done.
        if( !copy_all && panic_start == -1 ) // No panic?
          return;               // Then done copying after doing MIN_COPY_WORK
      }
      // Extra promotion check, in case another thread finished all copying
      // then got stalled before promoting.
      copy_check_and_promote( 0 ); // See if we can promote
    }


    // --- copy_slot_and_check -----------------------------------------------
    // Copy slot 'idx' from the old table to the new table.  If this thread
    // confirmed the copy, update the counters and check for promotion.
    //
    // Returns the result of reading the volatile _newchm, mostly as a
    // convenience to callers.  We come here with 1-shot copy requests
    // typically because the caller has found a Prime, and has not yet read
    // the _newchm volatile - which must have changed from null-to-not-null
    // before any Prime appears.  So the caller needs to read the _newchm
    // field to retry his operation in the new table, but probably has not
    // read it yet.
    private CHM copy_slot_and_check( int idx, long should_help ) {
      // We're only here because the caller saw a Prime, which implies a
      // table-copy is in progress.
      assert _newchm != null;
      if( copy_slot(idx) )      // Copy the desired slot
        copy_check_and_promote(1); // Record the slot copied
      // Generically help along any copy (except if called recursively from a helper)
      if( should_help != NO_KEY ) _sbs.help_copy();
      return _newchm;
    }

    // --- copy_check_and_promote --------------------------------------------
    private void copy_check_and_promote( int workdone ) {
      int oldlen = _keys.length;
      // We made a slot unusable and so did some of the needed copy work
      long copyDone = _copyDone;
      long nowDone = copyDone+workdone;
      assert nowDone <= oldlen;
      if( workdone > 0 ) {
        while( !_copyDoneUpdater.compareAndSet(this,copyDone,nowDone) ) {
          copyDone = _copyDone;   // Reload, retry
          nowDone = copyDone+workdone;
          assert nowDone <= oldlen;
        }
      }

      // Check for copy being ALL done, and promote.  Note that we might have
      // nested in-progress copies and manage to finish a nested copy before
      // finishing the top-level copy.  We only promote top-level copies.
      if( nowDone == oldlen &&   // Ready to promote this table?
          _sbs._chm == this && // Looking at the top-level table?
          // Attempt to promote
          _sbs.CAS_chm(_chm_offset,this,_newchm) )
        _sbs._last_resize_milli = System.currentTimeMillis();  // Record resize time for next check
    }

    // --- copy_slot ---------------------------------------------------------
    // Copy one Key from oldkvs[idx] to newkvs.

    // We need an accurate confirmed-copy count so that we know when we can
    // promote (if we promote the new table too soon, other threads may 'miss'
    // on values not-yet-copied from the old table).  We don't allow any direct
    // updates on the new table, unless they first happened to the old table -
    // so that any transition in the new table from null to not-null must have
    // been from a copy_slot (or other old-table overwrite) and not from a
    // thread directly writing in the new table.

    // A copy is confirmed if:
    // - it was marked (negative) in the old table, and we inserted it the new
    // - it was unmarked TOMBSTONE in the old, and we marked it.    
    private boolean copy_slot( int idx ) {
      long key;
      while( (key=_keys[idx]) >= NO_KEY ) {
        long mkey = ~(key==NO_KEY ? TOMBSTONE : key); // marked key
        if( CAS_key(idx, key, mkey) && mkey==~TOMBSTONE ) {
          //System.out.print("did "+idx+", ");
          return true;          // Did mark old table and no copy; report work-done.
        }
      }
      assert key < 0;                      // marked key
      if( key == ~TOMBSTONE ) return false; // No work to do on this slot
      // Copy to new table; if old was missing then we did the work
      boolean copied_into_new = !_newchm.set(~key,true); 
      // Stop any more attempts to move this key
      while( (key=_keys[idx]) != ~TOMBSTONE && CAS_key(idx, key, ~TOMBSTONE) ) ;
      //if( copied_into_new ) System.out.print("did "+idx+", ");
      //else { System.out.println("missed "+idx+" "+this); histo(); }
      return copied_into_new;      
    }
    
  } // End of CHM
}  // End SparseBitSet class
