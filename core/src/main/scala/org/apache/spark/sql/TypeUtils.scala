package org.apache.spark.sql

import java.sql.{Date, Timestamp}

import com.tribbloids.spookystuff.utils.TaggedUDT
import org.apache.spark.sql.catalyst.{CatalystTypeConverters, ScalaReflection}
import org.apache.spark.sql.types._

import scala.language.implicitConversions
import scala.reflect.ClassTag

/**
  * Created by peng on 03/06/16.
  */
object TypeUtils {

  import ScalaReflection.universe._

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

  def catalystTypeFor[T](implicit ttg: TypeTag[T]): DataType = {
    catalystTypeOptFor.getOrElse {
      throw new UnsupportedOperationException(s"cannot convert Scala type ${ttg.tpe} to Catalyst type")
    }
  }

  def catalystTypeOptFor[T](implicit ttg: TypeTag[T]): Option[DataType] = {
    ttg match {
      case TypeTag.Null => Some(NullType) //TODO: is it necessary?
      case _ =>
        try {
          val t = ScalaReflection.schemaFor[T].dataType
          Some(t)
        }
        catch {
          case e: Throwable => None
        }
    }
  }

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

  lazy val atomicTypePairs: Seq[(DataType, TypeTag[_])] = atomicExamples.map {
    v =>
      this.catalystTypeFor(v._2) -> v._2
  }

  lazy val atomicScalaTypeFor: Map[DataType, TypeTag[_]] = {
    Map(atomicTypePairs: _*)
  }

  /**
    * @param t if t is already an option won't yeild Option[ Option[_] ] again
    * @return
    */
  def selfOrOption(t: TypeTag[_]): Seq[TypeTag[_]] = {
    t match {
      case at: TypeTag[a] =>
        if (at.tpe <:< typeOf[Option[_]]) Seq[TypeTag[_]](at)
        else {
          implicit val att = at
          Seq[TypeTag[_]](at, typeTag[Option[a]])
        }
    }
  }

  def scalaTypesFor(dataType: DataType): Seq[TypeTag[_]] = {
    val baseOpt = baseScalaTypeOptFor(dataType)

    dataType match {
      case t if atomicScalaTypeFor.contains(t) =>
        baseOpt.toSeq.flatMap {selfOrOption}
      case udt: TaggedUDT[_] =>
        baseOpt.toSeq.flatMap {selfOrOption}
      case _ =>
        baseOpt.toSeq
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

  // CatalystType => ScalaType
  // used in ReflectionMixin to determine the exact function to:
  // 1. convert data from CatalystType to canonical Scala Type (and obtain its TypeTag)
  // 2. use the obtained TypeTag to get the specific function implementation and applies to the canonic Scala Type data.
  // 3. get the output TypeTag of the function, use it to generate the output DataType of the new Extraction.
  def baseScalaTypeOptFor(dataType: DataType): Option[TypeTag[_]] = {

    dataType match {
      case t if atomicScalaTypeFor.contains(t) =>
        atomicScalaTypeFor.get(t)
      case udt: TaggedUDT[_] =>
        Some(udt.ttg)
      case ArrayType(inner, _) =>
        val innerTagOpt = baseScalaTypeOptFor(inner)
        innerTagOpt.map {
          case at: TypeTag[a] =>
            implicit val att = at
            typeTag[Array[a]]
        }
      case MapType(key, value, _) =>
        val keyTag = baseScalaTypeOptFor(key)
        val valueTag = baseScalaTypeOptFor(value)
        val pairs = (keyTag, valueTag) match {
          case (Some(kt), Some(vt)) => Some(kt -> vt)
          case _ => None
        }

        pairs.map {
          pair =>
            (pair._1, pair._2) match {
              case (at: TypeTag[a], bt: TypeTag[b]) =>
                implicit val att = at
                implicit val btt = bt
                typeTag[Map[a, b]]
            }
        }
      case _ =>
        None
    }
  }

  def baseScalaTypeFor(dataType: DataType): TypeTag[_] = {
    baseScalaTypeOptFor(dataType).getOrElse {
      throw new UnsupportedOperationException(s"cannot convert Catalyst type $dataType to Scala type")
    }
  }


  //TODO: implement this
  // if its Others, use CatalystTypeConverters.getConverterForType and use scala reflection to get type parameter
  // if the previous doesn't work, then use hand crafted TypeTag
  // if all fails, throws an exception.

  //TODO: TypeCreator is not in Developer's API and usage is not recommended
  def typeToTypeTag[T](
                        tpe: Type,
                        mirror: reflect.api.Mirror[reflect.runtime.universe.type]
                      ): TypeTag[T] = {
    TypeTag(mirror, new reflect.api.TypeCreator {
      def apply[U <: reflect.api.Universe with Singleton](m: reflect.api.Mirror[U]) = {
        assert(m eq mirror, s"TypeTag[$tpe] defined in $mirror cannot be migrated to $m.")
        tpe.asInstanceOf[U#Type]
      }
    })
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

    implicit class ClassView[T](self: Class[T]) {

      val m = runtimeMirror(self.getClassLoader)

      def toType: Type = {

        val classSymbol = m.staticClass(self.getCanonicalName)
        val tpe = classSymbol.selfType
        tpe
      }

      def toTypeTag: TypeTag[T] = {
        TypeUtils.typeToTypeTag(toType, m)
      }
    }
  }
}