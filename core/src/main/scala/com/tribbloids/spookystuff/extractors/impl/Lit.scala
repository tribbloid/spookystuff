package com.tribbloids.spookystuff.extractors.impl

import com.tribbloids.spookystuff.extractors.GenExtractor.Static
import com.tribbloids.spookystuff.extractors._
import com.tribbloids.spookystuff.utils.UnreifiedScalaType
import org.apache.spark.ml.dsl.utils.MessageAPI
import org.apache.spark.sql.catalyst.ScalaReflection.universe.TypeTag
import org.apache.spark.sql.types._

/**
  * Created by peng on 7/3/17.
  */
//TODO: Message JSON conversion discard dataType info, is it wise?
case class Lit[T, +R](value: R, dataType: DataType) extends Static[T, R] {

  def valueOpt: Option[R] = Option(value)

  override lazy val toString = valueOpt
    .map {
      v =>
        //        MessageView(v).toJSON(pretty = false)
        "" + v
    }
    .getOrElse("NULL")

  override val partialFunction: PartialFunction[T, R] = Unlift({ _: T => valueOpt})
}

object Lit {

  def apply[T: TypeTag](v: T): Lit[FR, T] = {
    apply[FR, T](v, UnreifiedScalaType.apply[T])
  }

  def erased[T](v: T): Lit[FR, T] = {
    apply[FR, T](v, NullType)
  }

  lazy val NULL: Lit[FR, Null] = erased(null)
}