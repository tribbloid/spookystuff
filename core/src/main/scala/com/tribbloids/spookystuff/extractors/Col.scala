package com.tribbloids.spookystuff.extractors

import com.tribbloids.spookystuff.Const
import com.tribbloids.spookystuff.dsl.Implicits
import com.tribbloids.spookystuff.extractors.impl.{Extractors, Lit}
import org.apache.spark.ml.dsl.utils.MessageAPI
import org.apache.spark.sql.catalyst.ScalaReflection.universe.TypeTag

/**
  * Created by peng on 12/07/17.
  */
case class Col[T](
                   ex: Extractor[_ >: T]
                 ) extends MessageAPI {

  override def toString = ex.toString

  def resolveType(tt: DataType) = ex.resolveType(tt)
  def resolve(tt: DataType) = ex.resolve(tt)

  def value: T = ex.asInstanceOf[Lit[FR, T]].value

  override def toMessage = value
}

object Col {

  import Implicits._

  implicit def fromLiteral[T: TypeTag](v: T): Col[T] = {
    val ex = v match {
      //      case str: String if ctg <:< ClassTag(classOf[String]) =>
      case str: String =>
        val delimiter = Const.keyDelimiter
        val regex = (delimiter+"\\{[^\\{\\}\r\n]*\\}").r

        val result = if (regex.findFirstIn(str).isEmpty)
          Lit[String](str)
        else
          Extractors.ReplaceKeyExpr(str)

        result.asInstanceOf[Extractor[T]]
      case _ =>
        Lit(v)
    }

    Col[T](ex)
  }

  implicit def fromExtractor[T, R >: T](v: Extractor[R]): Col[T] = {
    Col[T](v)
  }

  implicit def fromSymbol[T](v: Symbol): Col[T] = {
    Col[T](v)
  }
}
