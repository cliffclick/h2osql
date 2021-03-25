package org.cliffc.sql;

import water.*;
import water.fvec.*;
import org.joda.time.DateTime;
import water.nbhm.NonBlockingHashMapLong;

/**
def q3 = count[message, comment, tag1, tag2:
    message_has_tag_tag(message, tag1)
    and comment_reply_of_message(comment, message)
    and comment_has_tag(comment, tag2)
    and not(comment_has_tag(comment, tag1))
    and tag1 != tag2
]
def message_has_tag_tag = comment_has_tag ; post_has_tag_tag

         Answer  Umbra 1 thrd  Umbra 48thrd   H2O 20thrd
SF0.1:   537142    0.0914 sec    0.0221 sec
SF1  :  6907213    1.7737 sec    0.1127 sec    0.8sec
SF10 : 70770955   15.429  sec    0.7805 sec   20.0sec
*/

public class TSMB3 implements TSMB.TSMBI {
  @Override public String name() { return "TSMB3"; }
  static final boolean PRINT_TIMING = true;

  // comment+post -1N-> tag1
  // cmt <-11-> cmt_replyof ->1N-> comment+post
  // cmt  -1N-> tag2 && tag1!=tag2
  
  // Query plan:
  // Hash comment->tag, post->tag
  // ForAll comments
  //   tag1 = [cp]tags[reply_of] // set of tags on a reply  , can be zero
  //   tag2 = ctags[cmt]         // set of tags on a comment, can be zero
  //   if( !tag1.overlap(tag2) )
  //     cnt += (tag1*tag2)
  

  @Override public long run() {
    long t0 = System.currentTimeMillis(), t;

    // Compute tags-per-comment/post.  Lookup by comment/post id.
    Vec cids0 = TSMB.COMMENT_HASTAG_TAG.vec("id");
    Vec ctgs  = TSMB.COMMENT_HASTAG_TAG.vec("hastag_tag");
    NonBlockingHashMapLong<AryInt> ctags = new BuildTags(TSMB.COMMENT.numRows()).doAll(cids0,ctgs)._tags;
    //TSMB.printA("Comments "+TSMB.COMMENT.numRows(),ctags);
    if( PRINT_TIMING ) { t=System.currentTimeMillis(); System.out.println("Build comment tags hashes "+(t-t0)+" msec"); t0=t; }
    
    Vec pids = TSMB.POST_HASTAG_TAG.vec("id");
    Vec ptgs = TSMB.POST_HASTAG_TAG.vec("hastag_tag");
    NonBlockingHashMapLong<AryInt> ptags = new BuildTags(TSMB.POST.numRows()).doAll(pids,ptgs)._tags;
    //TSMB.printA("Posts "+TSMB.POST.numRows(),ptags);
    if( PRINT_TIMING ) { t=System.currentTimeMillis(); System.out.println("Build post tags hashes "+(t-t0)+" msec"); t0=t; }

    // Count tags
    Vec cids1 = TSMB.COMMENT.vec("id");
    Vec crcs  = TSMB.COMMENT.vec("replyof_comment");
    Vec crps  = TSMB.COMMENT.vec("replyof_post");
    long cnt = new Count(ctags,ptags).doAll(cids1,crcs,crps)._cnt;
    if( PRINT_TIMING ) { t=System.currentTimeMillis(); System.out.println("Count matching tags "+(t-t0)+" msec"); t0=t; }
    
    return cnt;
  }


  private static class BuildTags extends MRTask<BuildTags> {
    transient NonBlockingHashMapLong<AryInt> _tags;
    final long _size;            // Uniques on outer hash
    BuildTags(long size) { _size=size; }
    @Override protected void setupLocal() { _tags = new NonBlockingHashMapLong<AryInt>((int)(_size>>1)); }
    @Override public void map(Chunk cids, Chunk ctgs) {
      Vec.Reader vids = cids.vec().new Reader();
      Vec.Reader vtgs = ctgs.vec().new Reader();
      long row0 = cids.start(), rowN = cids.vec().length();
      int row=0;
      // Skip leading rows with equal comment tags from prior chunk
      long prior = row0==0 ? -1 : vids.at8(row0-1);
      while( prior == cids.at8(row) ) row++;
      
      // Run to the chunk end and into the next chunk until the cid changes.
      // This means a single thread handles all of a comment id.
      AryInt tags=null;
      while( (row0+row) < rowN ) {
        long cid=vids.at8(row0+row);
        if( prior!=cid ) {
          if( row >= cids._len ) break;
          prior=cid;
          tags = new AryInt();
          AryInt old = _tags.putIfAbsent(cid,tags);
          assert old==null : "make id "+cid+", absrow "+(row0+row)+", start "+row0;       // Single insertion
        }
        int tag = (int)vtgs.at8(row0+row); // tag
        tags.push(tag);
        row++;
      }
    }
    @Override public void reduce( BuildTags bld ) {
      if( _tags != bld._tags )
        throw new RuntimeException("distributed reduce not implemented");
    }
  }

  private static class Count extends MRTask<Count> {
    long _cnt;
    final NonBlockingHashMapLong<AryInt> _ctags, _ptags;
    Count( NonBlockingHashMapLong<AryInt> ctags, NonBlockingHashMapLong<AryInt> ptags ) { _ctags=ctags; _ptags=ptags; }
    @Override public void map( Chunk cids, Chunk crcs, Chunk crps ) {
      long cnt=0;
      for( int i=0; i<cids._len; i++ ) {
        AryInt tag2s = _ctags.get(cids.at8(i));
        if( tag2s != null ) {
          AryInt tag1s = crps.isNA(i) ? _ctags.get(crcs.at8(i)) : _ptags.get(crps.at8(i));
          if( tag1s != null )
            for( int j=0; j<tag1s._len; j++ )
              if( tag2s.find(tag1s._es[j]) == -1 ) // no tag1 in tag2s
                cnt += tag2s._len;
        }
      }
      _cnt=cnt;
    }
    @Override public void reduce( Count C ) { _cnt += C._cnt; }
  }
}
