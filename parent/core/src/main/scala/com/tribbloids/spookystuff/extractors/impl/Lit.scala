package com.tribbloids.spookystuff.extractors.impl

import com.tribbloids.spookystuff.extractors
import com.tribbloids.spookystuff.extractors.GenExtractor.Static
import com.tribbloids.spookystuff.extractors._
import com.tribbloids.spookystuff.utils.IDMixin
import org.apache.spark.ml.dsl.utils.messaging.Relay_>>
import org.apache.spark.ml.dsl.utils.refl.UnreifiedObjectType
import org.apache.spark.sql.catalyst.ScalaReflection.universe.TypeTag
import org.apache.spark.sql.types._

/**
  * Created by peng on 7/3/17.
  */
//TODO: Message JSON conversion discard dataType info, is it wise?
case class Lit[T, +R](value: R, dataType: DataType) extends Static[T, R] with IDMixin {

  def _id = value

  def valueOpt: Option[R] = Option(value)

  override lazy val toString = valueOpt
    .map(_.toString)
    .getOrElse("NULL")

  override val partialFunction: PartialFunction[T, R] = Unlift { _: T =>
    valueOpt
  }
}

object Lit extends Relay_>>[Lit[_, _]] {

  def apply[T: TypeTag](v: T): Lit[FR, T] = {
    apply[FR, T](v, UnreifiedObjectType.summon[T])
  }

  def erased[T](v: T): Lit[FR, T] = {
    apply[FR, T](v, NullType)
  }

  lazy val NULL: Lit[FR, Null] = erased(null)

  override def toMessage_>>(v: Lit[_, _]): extractors.impl.Lit.M = v.value
}
