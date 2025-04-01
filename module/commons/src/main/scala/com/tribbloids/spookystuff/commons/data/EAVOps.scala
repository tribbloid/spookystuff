package com.tribbloids.spookystuff.commons.data

import com.tribbloids.spookystuff.commons.CommonUtils

case class EAVOps[T <: EAVLike](self: T)(
    implicit
    val schema: EAVSchema.Aux[T]
) {

  /**
    * favor the key-value pair in first operand attempt to preserve sequence as much as possible
    */
  def :++(that: T): T = {

    schema.EAV(CommonUtils.mergePreserveOrder(self.internal, that.internal))
  }

  /**
    * favor the key-value pair in second operand operands suffixed by : are reversed
    */
  final def ++:(other: T): T = {

    :++(other)
  }

  final def +=+(
      that: T
  ): T = {

    val _include: List[String] = (self.lookup.keys ++ that.lookup.keys).toList

    val result = _include.flatMap { key =>
      val vs = Seq(self, that).map { v =>
        v.lookup.get(key)
      }.flatten

      val mergedOpt = vs match {
        case Seq(v1, v2) =>
          require(v1 == v2, s"cannot merge, diverging values for $key: $v1 != $v2")
          vs.headOption
        case _ =>
          vs.headOption
      }

      mergedOpt.map { merged =>
        key -> merged
      }
    }

    schema.BuildFrom.seqInternal(result)
  }

  def updated(kvs: Magnets.AttrValueMag[Any]*): T = {
    ++:(schema(kvs*))
  }

  def drop(vs: Magnets.AttrMag*): T = schema.BuildFrom.seqInternal(
    (self.lookup -- vs.flatMap(_.names)).toSeq
  )

  def dropAll(vs: Iterable[Magnets.AttrMag]): T = drop(vs.toSeq*)

  def --(vs: Iterable[Magnets.AttrMag]): T = dropAll(vs)
}
