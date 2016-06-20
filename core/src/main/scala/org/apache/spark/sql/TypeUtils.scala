package org.apache.spark.sql

import java.sql.{Date, Timestamp}

import com.tribbloids.spookystuff.utils.AnyUDT
import org.apache.spark.ml.dsl.utils.FlowUtils
import org.apache.spark.sql.catalyst.{CatalystTypeConverters, ScalaReflection}
import org.apache.spark.sql.types._

import scala.language.implicitConversions
import scala.reflect.ClassTag

/**
  * Created by peng on 03/06/16.
  */
object TypeUtils {

  import ScalaReflection.universe
  import universe.TypeTag

  def typeTag[T: TypeTag] = ScalaReflection.universe.typeTag[T]
  def typeOf[T: TypeTag] = ScalaReflection.universe.typeOf[T]
  def getTypeTag[T: TypeTag](a: T) = implicitly[TypeTag[T]]

  //  type CanonicalArray[T] = Array[T]
  //  type CanonicalMap[A, B] = Map[A, B]

  // If reflection is used to find function based on DataType, it may encounter typecast error due to type inconsistency
  // (several Scala type corresponds to Catalyst DataType), in which case this should be used to pre-process the data to ensure
  // that they are compatible.
  // TODO: this implementation is slow, but barely works.
  def toCanonicalType(v: Any, dataType: DataType): Any = {
    val internal = CatalystTypeConverters.convertToCatalyst(v)
    CatalystTypeConverters.convertToScala(internal, dataType)
  }

  def catalystTypeFor[T](implicit ttg: TypeTag[T]): Option[DataType] = {
    try {
      val t = ScalaReflection.schemaFor[T].dataType
      Some(t)
    }
    catch {
      case e: Throwable => None
    }
  }

  def catalystTypeOrDefault[T](
                                default: DataType = NullType
                              )(
                                implicit ttg: TypeTag[T]
                              ): DataType = catalystTypeFor[T].getOrElse(default)

  lazy val atomicExamples: Seq[(Any, TypeTag[_])] = {

    implicit def pairFor[T: TypeTag](v: T): (T, TypeTag[T]) = {
      v -> this.getTypeTag[T](v)
    }

    val result = Seq[(Any, TypeTag[_])](
      Array(0: Byte),
      false,
      new Date(0),
      0.0,
      0: Float,
      0: Byte,
      0: Int,
      0L,
      0: Short,
      "0",
      new Timestamp(0)
    )
    result
  }

  lazy val atomicTypePairs: Seq[(Option[DataType], TypeTag[_])] = atomicExamples.map {
    v =>
      this.catalystTypeFor(v._2) -> v._2
  }

  lazy val atomicScalaTypeFor: Map[DataType, TypeTag[_]] = {
    Map(
      atomicTypePairs.flatMap(
        v =>
          v._1.map(vv =>vv -> v._2)
      ): _*
    )
  }

  def selfAndOptionType(t: TypeTag[_]): Set[TypeTag[_]] = {
    t match {
      case at: TypeTag[a] =>
        if (at.tpe <:< typeOf[Option[_]]) Set[TypeTag[_]](at)
        else {
          implicit val att = at
          Set[TypeTag[_]](at, typeTag[Option[a]])
        }
    }
  }

  def scalaTypesFor(dataType: DataType): Set[TypeTag[_]] = {
    val base = baseTypesFor(dataType)

    dataType match {
      case t if atomicScalaTypeFor.contains(t) =>
        base.flatMap {selfAndOptionType}
      case udt: AnyUDT[_] =>
        base.flatMap {selfAndOptionType}
      case _ =>
        base
    }
  }

  // used in ReflectionMixin to determine the exact function to:
  // 1. convert data from CatalystType to canonical Scala Type (and obtain its TypeTag)
  // 2. use the obtained TypeTag to get the specific function implementation and applies to the canonic Scala Type data.
  // 3. get the output TypeTag of the function, use it to generate the output DataType of the new Extraction.
  def baseTypesFor(dataType: DataType): Set[TypeTag[_]] = {

    dataType match {
      case t if atomicScalaTypeFor.contains(t) =>
        atomicScalaTypeFor.get(t).toSet
      case udt: AnyUDT[_] =>
        Some(udt.ttg).toSet
      case ArrayType(inner, _) =>
        val innerTagOpt = baseTypesFor(inner)
        innerTagOpt.map {
          case at: TypeTag[a] =>
            implicit val att = at
            typeTag[Array[a]]
        }
      case MapType(key, value, _) =>
        val keyTag = baseTypesFor(key)
        val valueTag = baseTypesFor(value)
        val cartesian = FlowUtils.cartesianProductSet(List(keyTag, valueTag))

        val pairs: Set[(TypeTag[_], TypeTag[_])] = cartesian.map {
          list =>
            list.head -> list.last
        }

        pairs.flatMap[TypeTag[_], Set[TypeTag[_]]] {
          pair =>
            (pair._1, pair._2) match {
              case (at: TypeTag[a], bt: TypeTag[b]) =>
                implicit val att = at
                implicit val btt = bt
                Set(typeTag[Map[a, b]])
            }
        }
      case _ =>
        Set.empty
    }

    //TODO: implement this
    // if its Others, use CatalystTypeConverters.getConverterForType and use scala reflection to get type parameter
    // if the previous doesn't work, then use hand crafted TypeTag
    // if all fails, throws an exception.
  }

  object Implicits {
    implicit class ClassTagViews[T](self: ClassTag[T]) {

      //  def toTypeTag: TypeTag[T] = TypeTag.apply()

      def toClass: Class[T] = self.runtimeClass.asInstanceOf[Class[T]]
    }

    implicit class TypeTagViews[T](self: TypeTag[T]) {

      def toClassTag: ClassTag[T] = ClassTag(clazz)

      def toClass: Class[T] = clazz.asInstanceOf[Class[T]]

      def clazz: ScalaReflection.universe.RuntimeClass = self.mirror.runtimeClass(self.tpe)
    }
  }
}