package com.tribbloids.spookystuff.utils

import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.types._
import org.slf4j.LoggerFactory

import scala.language.implicitConversions

object TypeUtils {

  import ScalaReflection.universe._

  // If reflection is used to find function based on DataType, it may encounter typecast error due to type inconsistency
  // (several Scala type corresponds to Catalyst DataType), in which case this should be used to pre-process the data to ensure
  // that they are compatible.
  // TODO: this implementation is slow, but barely works.
  //  def toCanonicalType(v: Any, dataType: DataType): Any = {
  //    val internal = CatalystTypeConverters.convertToCatalyst(v)
  //    CatalystTypeConverters.convertToScala(internal, dataType)
  //  }

  def getTypeTag[T: TypeTag](a: T) = implicitly[TypeTag[T]]

  def catalystTypeOptFor[T](implicit ttg: TypeTag[T]): Option[DataType] = {
    try {
      val result = catalystTypeFor[T](ttg)
      Some(result)
    }
    catch {
      case e: Throwable =>
        LoggerFactory.getLogger(this.getClass).warn(
          s"cannot convert Scala type $ttg to Catalyst type:\n" + e.getLocalizedMessage
        )
        None
    }
  }

  def catalystTypeFor[T](implicit ttg: TypeTag[T]): DataType = {
    ttg match {
      case TypeTag.Null =>
        NullType
      case _ =>
        ScalaReflection.schemaFor[T](ttg).dataType
    }
  }

  /**
    * @param t if t is already an option won't yeild Option[ Option[_] ] again
    * @return
    */
  private def selfOrOption(t: TypeTag[_]): Seq[TypeTag[_]] = {
    t match {
      case at: TypeTag[a] =>
        if (at.tpe <:< typeOf[Option[_]]) Seq[TypeTag[_]](at)
        else {
          implicit val att = at
          Seq[TypeTag[_]](at, typeTag[Option[a]])
        }
    }
  }

  def getParameter_ReturnTypes(symbol: MethodSymbol, impl: Type) = {

    val signature = symbol.typeSignatureIn(impl)
    val result = methodSignatureToParameter_ReturnTypes(signature)
    result
  }

  private def methodSignatureToParameter_ReturnTypes(tpe: Type): (List[List[Type]], Type) = {
    tpe match {
      case n: NullaryMethodType =>
        Nil -> n.resultType
      case m: MethodType =>
        val paramTypes: List[Type] = m.params.map(_.typeSignatureIn(tpe))
        val downstream = methodSignatureToParameter_ReturnTypes(m.resultType)
        downstream.copy(_1 = List(paramTypes) ++ methodSignatureToParameter_ReturnTypes(m.resultType)._1)
      case _ =>
        Nil -> tpe
    }
  }

  def fitIntoArgs(t1: Option[Seq[Type]], t2: Option[Seq[Type]]): Boolean = {
    (t1, t2) match {
      case (Some(tt1), Some(tt2)) =>
        if (tt1.size != tt2.size) false
        else {
          val fa: Boolean = tt1.zip(tt2).forall(
            tuple =>
              tuple._1 <:< tuple._2
          )
          fa
        }
      case (None, None) =>
        true
      case _ => false
    }
  }

  //TODO: TypeCreator is not in Developer's API and usage is not recommended
  def createTypeTag[T](
                        tpe: Type,
                        mirror: reflect.api.Mirror[reflect.runtime.universe.type]
                      ): TypeTag[T] = {

    TypeTag(
      mirror,
      new reflect.api.TypeCreator {
        def apply[U <: reflect.api.Universe with Singleton](m: reflect.api.Mirror[U]) = {
//          assert(m eq mirror, s"TypeTag[$tpe] defined in $mirror cannot be migrated to $m.")
          tpe.asInstanceOf[U#Type]
        }
      }
    )
  }
}