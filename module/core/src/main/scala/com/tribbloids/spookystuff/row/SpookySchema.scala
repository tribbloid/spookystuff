package com.tribbloids.spookystuff.row

import com.tribbloids.spookystuff.SpookyContext
import com.tribbloids.spookystuff.execution.*

//this is a special StructType that carries more metadata
//TODO: override sqlType, serialize & deserialize to compress into InternalRow
case class SpookySchema(ec: ExecutionContext)
//                       (
//    implicit
//    val ordering: Ordering[D] // should be only useful & summoned in explore()
////    val encoder: TypedEncoder[D] TODO: will be made typed in the next stage, TypedEncoder  for
//)
{

  def ctx: SpookyContext = ec.ctx
}
