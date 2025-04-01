package com.tribbloids.spookystuff.linq

import ai.acyclic.prover.commons.compat.TupleX
import com.tribbloids.spookystuff.linq.Record

@Deprecated
case class SemiRow[S1, S2 <: TupleX]( // TODO: this is unnecessary, tuple is good enough
    static: S1,
    dynamic: Record[S2]
) {

  // this is reserved for semi-static schema, like those used in GraphFrame
}
