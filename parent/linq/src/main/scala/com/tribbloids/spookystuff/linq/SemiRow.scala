package com.tribbloids.spookystuff.linq

import com.tribbloids.spookystuff.linq.Linq.Row

@Deprecated
case class SemiRow[S1, S2 <: Tuple]( // TODO: this is unnecessary, tuple is good enough
    static: S1,
    dynamic: Row[S2]
) {

  // this is reserved for semi-static schema, like those used in GraphFrame
}
