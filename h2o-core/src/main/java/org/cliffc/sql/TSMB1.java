package org.cliffc.sql;

import water.*;
import water.fvec.*;
import org.joda.time.DateTime;
import water.nbhm.NonBlockingHashMapLong;

/**
def q1 = count[city, country, person, forum, post, comment, tag, tagclass:
    city_is_part_of_country(city, country)
    and person_is_located_in_place(person, city)
    and forum_has_moderator_person(forum, person)
    and forum_container_of_post(forum, post)
    and comment_reply_of_post(comment, post)
    and comment_has_tag(comment, tag)
    and tag_has_type_tagclass(tag, tagclass)
]

          Answer  Umbra 1 thrd  Umbra 48thrd   H2O 20thrd
SF0.1:    119790    0.0694 sec    0.1485 sec    0.010 sec
SF1  :   1477484    1.1392 sec    0.4043 sec    0.027 sec
SF10 :  14947019   10.7193 sec    0.8437 sec    0.287 sec
*/

public class TSMB1 implements TSMB.TSMBI {
  @Override public String name() { return "TSMB1"; }
  static final boolean PRINT_TIMING = false;

  // forum -11-> person -1N-> city -1N-> country
  // post -1N-> forum        // 
  // comment_reply_of -1N-> post
  // comment -NN-> tag -11-> tagclass

  // Chaining:
  // comment -11-> comment_reply_of -1N-> post -1N-> forum -11-> person -1N-> city -1N-> country
  // comment -NN-> tag -11-> tagclass
  
  // Query plan:
  // Sparse bit set of comments/replyof.
  // Count tags of said comments.
  

  @Override public long run() {
    long t0 = System.currentTimeMillis(), t;

    // Compute comment IDs that are replyof_post
    Vec cids = TSMB.COMMENT.vec("id");
    Vec cres = TSMB.COMMENT.vec("replyof_post");
    NonBlockingHashMapLong replys = new BuildReplys().doAll(cids,cres)._replys;
    if( PRINT_TIMING ) { t=System.currentTimeMillis(); System.out.println("Build SBS comment replys "+(t-t0)+" msec"); t0=t; }
    
    // Count tags
    Vec cidts = TSMB.COMMENT_HASTAG_TAG.vec("id");
    long cnt = new Count(replys).doAll(cidts)._cnt;
    if( PRINT_TIMING ) { t=System.currentTimeMillis(); System.out.println("Count matching tags "+(t-t0)+" msec"); t0=t; }
    
    return cnt;
  }
  
  private static class BuildReplys extends MRTask<BuildReplys> {
    transient NonBlockingHashMapLong _replys;
    @Override protected void setupLocal() { _replys = new NonBlockingHashMapLong((int)(_fr.numRows()*2)); }
    @Override public void map( Chunk cids, Chunk cres ) {
      for( int i=0; i<cids._len; i++ )
        if( !cres.isNA(i) )
          _replys.put(cids.at8(i),"");
    }
    @Override public void reduce( BuildReplys bld ) {
      if( _replys != bld._replys )
        throw new RuntimeException("distributed reduce not implemented");
    }
  }

  private static class Count extends MRTask<Count> {
    long _cnt;
    final NonBlockingHashMapLong _replys;
    Count( NonBlockingHashMapLong replys ) { _replys=replys; }
    @Override public void map( Chunk cids ) {
      long cnt=0;
      for( int i=0; i<cids._len; i++ )
        if( _replys.containsKey(cids.at8(i)) )
          cnt++;
      _cnt=cnt;
    }
    @Override public void reduce( Count C ) { _cnt += C._cnt; }
  }
}
