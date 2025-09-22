package com.tribbloids.spookystuff.commons.refl

import ai.acyclic.prover.commons.spark.serialization.{SerializerEnv, SerializerOverride}

import scala.language.implicitConversions
import scala.reflect.ClassTag

/**
  * interface that unifies TypeTag, ClassTag, Class & DataType Also a subclass of Spark SQL DataType but NOT recommended
  * to use directly in DataFrame, can cause compatibility issues. either use tryReify to attempt converting to a native
  * DataType. Or use UnoptimizedScalaUDT (which is abandoned in Spark 2.x) Will be simplified again once Spark 2.2
  * introduces UserDefinedType V2.
  */
//TODO: change to ThreadLocal to be faster?
//TODO: should be "WeakTypeMagnet", erasure may happen
//TODO: should have a codec
trait TypeMagnet[T] extends Serializable {

  import TypeMagnet.universe.*

  override def toString: String = asType.toString

  @transient lazy val mirror: Mirror = asTypeTag.mirror

  @transient final lazy val asTypeTag: TypeTag[T] = {
    _typeTag
  }

  def _typeTag: TypeTag[T]

  @transient lazy val asType: Type = {
    TypeMagnet.localTypeOf(asTypeTag)
  }
  @transient lazy val asClass: Class[T] = {
    val result = mirror.runtimeClass(asType).asInstanceOf[Class[T]]
    result
  }
  @transient lazy val asClassTag: ClassTag[T] = {
    ClassTag(asClass)
  }

  //  def reifyOrNullType = tryReify.getOrElse { NullType }

  // see [SPARK-8647], this achieves the needed constant hash code without declaring singleton
  final override def hashCode: Int = asClass.hashCode()

  final override def equals(v: Any): Boolean = {
    if (v == null) return false
    v match {
      case vv: TypeMagnet[_] =>
        (this.asClass == vv.asClass) && (this.asType =:= vv.asType)
      case _ =>
        false
    }
  }

  object utils {

    lazy val companionObject: Any = {
      val mirror = TypeMagnet.this.mirror
      val companionMirror = mirror.reflectModule(asType.typeSymbol.companion.asModule)
      companionMirror.instance
    }

    lazy val baseCompanionObjects: Seq[Any] = {

      val mirror = TypeMagnet.this.mirror
      val supers = asType.typeSymbol.asClass.baseClasses

      supers.flatMap { ss =>
        scala.util.Try {
          val companionMirror = mirror.reflectModule(ss.companion.asModule)
          companionMirror.instance
        }.toOption
      }
    }
  }
}

object TypeMagnet extends FromClassTagMixin {

  import universe.*

  def localTypeOf[T: TypeTag]: `Type` = {
    val tag = implicitly[TypeTag[T]]
    tag.in(tag.mirror).tpe.dealias
  }

  protected class FromTypeTag[T](@transient protected val __typeTag: TypeTag[T]) extends TypeMagnet[T] {
    // TODO: dropped in Scala 3, switch to manifest or TASTy typetag

    {
      typeTag_ser
    }

    lazy val typeTag_ser: SerializerOverride[TypeTag[T]] = {

      SerializerOverride(__typeTag, SerializerEnv.Default.javaOverride)
    }

    def _typeTag: TypeTag[T] = typeTag_ser.value
  }

  object FromTypeTag extends CachedBuilder[TypeTag] {

    override def createNew[T](v: TypeTag[T]): TypeMagnet[T] = new FromTypeTag[T](v)
  }

  implicit def _fromTypeTag[T](v: TypeTag[T]): TypeMagnet[T] = FromTypeTag.apply(v)
  implicit def __fromTypeTag[T](
      implicit
      v: TypeTag[T]
  ): TypeMagnet[T] = FromTypeTag.apply(v)

  def fromType[T](tpe: Type, mirror: Mirror): FromTypeTag[T] = {

    val ttg = TypeUtils.createTypeTag_fast[T](tpe, mirror)

    new FromTypeTag(ttg)
  }

  def summon[T](
      implicit
      ev: TypeMagnet[T]
  ): TypeMagnet[T] = ev

  def getRuntimeType(v: Any): TypeMagnet[?] = {
    Unerase
      .get(v)
      .map(v => FromTypeTag(v))
      .getOrElse(FromClass(v.getClass))
  }
}
