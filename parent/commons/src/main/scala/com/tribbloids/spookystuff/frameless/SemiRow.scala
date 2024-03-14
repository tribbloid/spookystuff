package com.tribbloids.spookystuff.frameless

case class SemiRow[S1, S2 <: Tuple](
    static: S1,
    dynamic: TypedRow[S2]
) {

  // this is reserved for semi-static schema, like those used in GraphFrame
}
