package com.tribbloids.spookystuff.row

import com.tribbloids.spookystuff.SpookyContext
import com.tribbloids.spookystuff.execution._

import scala.language.implicitConversions

//this is a special StructType that carries more metadata
//TODO: override sqlType, serialize & deserialize to compress into InternalRow
case class SpookySchema[D](ec: SpookyExecutionContext)(
    implicit
    val ordering: Ordering[D] // D should always be sortable for the next exploring
//    val encoder: TypedEncoder[D] TODO: deferred to next stage, TypedEncoder is only required for
) {

  def spooky: SpookyContext = ec.spooky
}
