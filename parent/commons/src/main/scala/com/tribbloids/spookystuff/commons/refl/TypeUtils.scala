package com.tribbloids.spookystuff.commons.refl

import org.apache.spark.SparkException
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.types._

import scala.util.Failure

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

  def summon[T: TypeTag](a: T): TypeTag[T] = implicitly[TypeTag[T]]

  def tryCatalystTypeFor[T](
      implicit
      ttg: TypeTag[T]
  ): scala.util.Try[DataType] = {
    scala.util
      .Try {
        if (ttg == TypeTag.Null) NullType
        else {
          ScalaReflection.schemaFor[T](ttg).dataType
        }
      }
      .recoverWith {
        case e: Exception =>
          Failure(
            new SparkException(
              s"Cannot find catalyst type for $ttg",
              e
            )
          )
      }
  }

  /**
    * @param t
    *   if t is already an option won't yeild Option[ Option[_] ] again
    * @return
    */
//  private def selfAndOptionTypeIfNotAlready(t: TypeTag[_]): Seq[TypeTag[_]] = {
//    t match {
//      case at: TypeTag[a] =>
//        if (at.tpe <:< typeOf[Option[_]]) Seq[TypeTag[_]](at)
//        else {
//          implicit val att: TypeTag[a] = at
//          Seq[TypeTag[_]](at, typeTag[Option[a]])
//        }
//    }
//  }

  def getParameter_ReturnTypes(
      symbol: MethodSymbol,
      impl: Type
  ): (List[List[Type]], Type) = {

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
          val fa: Boolean = tt1
            .zip(tt2)
            .forall(tuple => tuple._1 <:< tuple._2)
          fa
        }
      case (None, None) =>
        true
      case _ => false
    }
  }

  // TODO: TypeCreator is not in Developer's API and usage is not recommended
  def createTypeTag_fast[T](
      tpe: Type,
      mirror: Mirror
  ): TypeTag[T] = {
    TypeTag.apply(
      mirror,
      NaiveTypeCreator(tpe)
    )
  }

  // TODO: this needs improvement due to:
  // https://stackoverflow.com/questions/59473734/in-scala-2-12-why-none-of-the-typetag-created-in-runtime-is-serializable
//  def createTypeTag_slowButSerializable[T](
//      tpe: Type,
//      mirror: Mirror
//  ): TypeTag[T] = {
//
//    val toolbox = scala.tools.reflect.ToolBox(mirror).mkToolBox()
//
//    val tree = toolbox.parse(s"scala.reflect.runtime.universe.typeTag[$tpe]")
//    val result = toolbox.eval(tree).asInstanceOf[TypeTag[T]]
//
//    result
//  }

  case class NaiveTypeCreator(tpe: Type) extends reflect.api.TypeCreator {

    def apply[U <: reflect.api.Universe with Singleton](m: reflect.api.Mirror[U]): U#Type = {
      //          assert(m eq mirror, s"TypeTag[$tpe] defined in $mirror cannot be migrated to $m.")
      tpe.asInstanceOf[U#Type]
    }
  }
}
