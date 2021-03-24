package org.cliffc.sql;

import water.*;
import water.fvec.*;
import org.joda.time.DateTime;
import water.nbhm.NonBlockingHashMapLong;

/**
def q5 = count[cityA, cityB, cityC, country, pA, pB, pC:
    city_is_part_of_country(cityA, country)
    and city_is_part_of_country(cityB, country)
    and city_is_part_of_country(cityC, country)

    and person_is_located_in_place(pA, cityA)
    and person_is_located_in_place(pB, cityB)
    and person_is_located_in_place(pC, cityC)

    and person_knows_person(pA, pB)
    and person_knows_person(pB, pC)
    and person_knows_person(pC, pA)
]

Umbra,                             H2O
SF0.1:   0.117	   30456
SF1  :  2.3345	  753570
SF10 : 25.6234	15028644
*/

public class Delve5 implements TSMB.Delve {
  @Override public String name() { return "Delve5"; }
  static final boolean PRINT_TIMING = false;

  // Query plan:
  // Extra info: person-knows-person is symmetric.
  //
  // find triangles A<->B<->C<->A, where all 3 are in same country.
  
  
  @Override public long run() {
    long t0 = System.currentTimeMillis(), t;
    return -1;
  }
}
