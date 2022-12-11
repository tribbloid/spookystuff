package org.apache.spark.ml.dsl.utils.data

import com.tribbloids.spookystuff.utils.CommonUtils

case class EAVOps[T <: EAV](self: T)(
    implicit
    val sys: EAVSystem.Aux[T]
) {

  /**
    * favor the key-value pair in first operand attempt to preserve sequence as much as possible
    */
  def :++(that: T): T = {

    sys._EAV(CommonUtils.mergePreserveOrder(self.internal, that.internal))
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

    val _include: List[String] = (self.asMap.keys ++ that.asMap.keys).toList

    val result = _include.flatMap { key =>
      val vs = Seq(self, that).map { v =>
        v.asMap.get(key)
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

    sys.From.tuple(result: _*)
  }

  def updated(kvs: Magnets.AttrValueMag[self.Bound]*): T = {
    ++:(sys.From(kvs: _*))
  }

  def drop(vs: Magnets.AttrMag*): T = sys.From.iterable(self.asMap -- vs.flatMap(_.names))

  def dropAll(vs: Iterable[Magnets.AttrMag]): T = drop(vs.toSeq: _*)

  def --(vs: Iterable[Magnets.AttrMag]): T = dropAll(vs)
}
