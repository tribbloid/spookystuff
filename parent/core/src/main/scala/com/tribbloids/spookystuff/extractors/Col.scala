package com.tribbloids.spookystuff.extractors

import com.tribbloids.spookystuff.Const
import com.tribbloids.spookystuff.extractors.impl.{Extractors, Lit}
import com.tribbloids.spookystuff.relay.{ProtoAPI, RootTagged, TreeIR}
import org.apache.spark.sql.catalyst.ScalaReflection.universe.TypeTag

import scala.language.implicitConversions

/**
  * Created by peng on 12/07/17.
  */
object Col {

  import com.tribbloids.spookystuff.dsl.DSL._

  implicit def fromLiteral[T: TypeTag, V](v: V)(
      implicit
      ev: V => T
  ): Col[T] = {
    val ex = v match {
      //      case str: String if ctg <:< ClassTag(classOf[String]) =>
      case str: String =>
        val delimiter = Const.keyDelimiter
        val regex = (delimiter + "\\{[^\\{\\}\r\n]*\\}").r

        val result =
          if (regex.findFirstIn(str).isEmpty)
            Lit[String](str)
          else
            Extractors.ReplaceKeyExpr(str)

        result.asInstanceOf[Extractor[T]]
      case _ =>
        Lit(ev(v))
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

case class Col[T](
    ex: Extractor[_ >: T]
) extends ProtoAPI
    with RootTagged {

  override def toString: String = this.treeText

  def resolveType(tt: DataType): DataType = ex.resolveType(tt)
  def resolve(tt: DataType): PartialFunction[FR, _ >: T] = ex.resolve(tt)

  def value: T = {
    ex match {
      case v: Lit[_, T] => v.value
      case _            => throw new UnsupportedOperationException("Not a literal")
    }
  }

  override lazy val rootTag: String = ex.productPrefix

  override def toMessage_>> = {
    val result = ex match {
      case v: Lit[_, T] =>
        TreeIR.Builder(Some(rootTag)).leaf(v.value)
      case _ => GenExtractor.toMessage_>>(ex)
    }
    result
  }
}
