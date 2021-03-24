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
SF0.1:    119790    0.0694 sec    0.1485 sec     sec
SF1  :   1477484    1.1392 sec    0.4043 sec     sec
SF10 :  14947019   10.7193 sec    0.8437 sec     sec
*/

public class TSMB1 implements TSMB.TSMBI {
  @Override public String name() { return "TSMB1"; }
  static final boolean PRINT_TIMING = false;
  
  // Extra info: person-knows-person is symmetric.

  // Query plan:
  
  @Override public long run() {
    long t0 = System.currentTimeMillis(), t;

    if( PRINT_TIMING ) { t=System.currentTimeMillis(); System.out.println("ForAll P1,P2,P3 "+(t-t0)+" msec"); t0=t; }
    
    return -1;
  }

}
