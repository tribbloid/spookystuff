package com.tribbloids.spookystuff.extractors

import com.tribbloids.spookystuff.extractors.impl.Lit
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
    val ex = Lit(ev(v))

    Col[T](ex)
  }

  implicit def fromExtractor[T, R >: T](v: Extractor[R]): Col[T] = {
    Col[T](v)
  }

  implicit def fromSymbol[T](v: Symbol): Col[T] = {
    Col[T](v)
  }

  implicit def unbox[T](v: Col[T]): Extractor[_ >: T] = v.ex
}

case class Col[T](
    ex: Extractor[_ >: T]
) extends ProtoAPI
    with RootTagged {

  override def toString: String = this.treeText

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
