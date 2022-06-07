package org.apache.spark.ml.dsl.utils.refl

import java.sql.{Date, Timestamp}

import com.tribbloids.spookystuff.utils.CachingUtils.ConcurrentCache
import com.tribbloids.spookystuff.utils.serialization.SerDeOverride
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.catalyst.ScalaReflection.universe._
import org.apache.spark.sql.types._

import scala.collection.Map
import scala.language.{existentials, higherKinds, implicitConversions}
import scala.reflect.ClassTag

/**
  * interface that unifies TypeTag, ClassTag, Class & DataType Also a subclass of Spark SQL DataType but NOT recommended
  * to use directly in DataFrame, can cause compatibility issues. either use tryReify to attempt converting to a native
  * DataType. Or use UnoptimizedScalaUDT (which is abandoned in Spark 2.x) Will be simplified again once Spark 2.2
  * introduces UserDefinedType V2.
  */
//TODO: change to ThreadLocal to bypass thread safety?
//TODO: this should be a codec
trait ScalaType[T] extends ReflectionLock with Serializable {

  override def toString: String = asType.toString

  @transient lazy val mirror: Mirror = asTypeTag.mirror

  @transient final lazy val asTypeTag: TypeTag[T] = locked {
    _typeTag
  }

  def _typeTag: TypeTag[T]

  @transient lazy val asType: Type = locked {
    ScalaReflection.localTypeOf(asTypeTag)
  }
  @transient lazy val asClass: Class[T] = locked {
    val result = mirror.runtimeClass(asType).asInstanceOf[Class[T]]
    result
  }
  @transient lazy val asClassTag: ClassTag[T] = locked {
    ClassTag(asClass)
  }

  @transient lazy val tryReify: scala.util.Try[DataType] = {
    TypeUtils
      .tryCatalystTypeFor(asTypeTag)
  }

  def reify: DataType = tryReify.get

  def asCatalystType: DataType = tryReify.getOrElse {
    new UnreifiedObjectType[T]()(this)
  }

  //  def reifyOrNullType = tryReify.getOrElse { NullType }

  // see [SPARK-8647], this achieves the needed constant hash code without declaring singleton
  final override def hashCode: Int = asClass.hashCode()

  final override def equals(v: Any): Boolean = {
    if (v == null) return false
    v match {
      case vv: ScalaType[_] =>
        (this.asClass == vv.asClass) && (this.asType =:= vv.asType)
      case _ =>
        false
    }
  }

  object utils {

    lazy val companionObject: Any = {
      val mirror = ScalaType.this.mirror
      val companionMirror = mirror.reflectModule(asType.typeSymbol.companion.asModule)
      companionMirror.instance
    }

    lazy val baseCompanionObjects: Seq[Any] = {

      val mirror = ScalaType.this.mirror
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

sealed abstract class ScalaType_Level1 {

  trait _Ctg[T] extends ScalaType[T] {

    protected def _classTag: ClassTag[T]
    @transient final override lazy val asClassTag: ClassTag[T] = _classTag

    override lazy val asClass: Class[T] = {
      asClassTag.runtimeClass.asInstanceOf[Class[T]]
    }

    def classLoader: ClassLoader = asClass.getClassLoader

    @transient override lazy val mirror: Mirror = {
      val loader = classLoader
      runtimeMirror(loader)
    }
    //    def mirror = ReflectionUtils.mirrorFactory.get()

    @transient override lazy val asType: Type = locked {
      //      val name = _class.getCanonicalName

      val classSymbol = getClassSymbol(asClass)
      val tpe = classSymbol.selfType
      tpe
    }

    def getClassSymbol(_class: Class[_]): ClassSymbol = {

      try {
        mirror.classSymbol(_class)
      } catch {
        case e: AssertionError =>
          val superclass = Seq(_class.getSuperclass).filter { v =>
            v != classOf[AnyRef]
          }
          val interfaces = _class.getInterfaces

          mirror.classSymbol((superclass ++ interfaces).head)
      }
    }

    override def _typeTag: TypeTag[T] = {
      TypeUtils.createTypeTag_fast(asType, mirror)
    }
  }

  protected class FromClassTag[T](val _classTag: ClassTag[T]) extends _Ctg[T]

  trait CachedBuilder[I[_]] extends Serializable {

    def createNew[T](v: I[T]): ScalaType[T]

    protected lazy val cache: ConcurrentCache[I[_], ScalaType[_]] = ConcurrentCache[I[_], ScalaType[_]]()

    final def apply[T](
        implicit
        v: I[T]
    ): ScalaType[T] = {
      cache
        .getOrElseUpdate(
          v,
          createNew[T](v)
        )
        .asInstanceOf[ScalaType[T]]
    }
  }

  object FromClass extends CachedBuilder[Class] {

    override def createNew[T](v: Class[T]): ScalaType[T] = new FromClassTag(ClassTag(v))
  }

  // TODO: how to get rid of these boilerplates?
  implicit def _fromClass[T](v: Class[T]): ScalaType[T] = FromClass(v)
  implicit def __fromClass[T](
      implicit
      v: Class[T]
  ): ScalaType[T] = FromClass(v)
}

sealed abstract class ScalaType_Level2 extends ScalaType_Level1 {

  object FromClassTag extends CachedBuilder[ClassTag] {

    override def createNew[T](v: ClassTag[T]): ScalaType[T] = new FromClassTag(v)
  }

  implicit def _fromClassTag[T](v: ClassTag[T]): ScalaType[T] = FromClassTag(v)
  implicit def __fromClassTag[T](
      implicit
      v: ClassTag[T]
  ): ScalaType[T] = FromClassTag(v)
}

object ScalaType extends ScalaType_Level2 {

  trait _Ttg[T] extends ScalaType[T] {}

  protected class FromTypeTag[T](@transient protected val __typeTag: TypeTag[T]) extends _Ttg[T] {

    {
      typeTag_ser
    }

    lazy val typeTag_ser: SerDeOverride[TypeTag[T]] = {

      SerDeOverride(__typeTag, SerDeOverride.Default.javaOverride)
    }

    def _typeTag: TypeTag[T] = typeTag_ser.value
  }

  object FromTypeTag extends CachedBuilder[TypeTag] {

    override def createNew[T](v: TypeTag[T]): ScalaType[T] = new FromTypeTag[T](v)
  }

  implicit def _fromTypeTag[T](v: TypeTag[T]): ScalaType[T] = FromTypeTag.apply(v)
  implicit def __fromTypeTag[T](
      implicit
      v: TypeTag[T]
  ): ScalaType[T] = FromTypeTag.apply(v)

  def fromType[T](tpe: Type, mirror: Mirror): FromTypeTag[T] = {

    val ttg = TypeUtils.createTypeTag_slowButSerializable[T](tpe, mirror)

    new FromTypeTag(ttg)
  }

  def summon[T](
      implicit
      ev: ScalaType[T]
  ): ScalaType[T] = ev

  def getRuntimeType(v: Any): ScalaType[_] = {
    v match {
      case v: RuntimeTypeOverride => v.runtimeType
      case _                      => v.getClass
    }
  }

  object FromCatalystType {

    lazy val atomicExamples: Seq[(Any, TypeTag[_])] = {

      implicit def pairFor[T: TypeTag](v: T): (T, TypeTag[T]) = {
        v -> TypeUtils.summon[T](v)
      }

      val result = Seq[(Any, TypeTag[_])](
        Array(0: Byte),
        false,
        new Date(0),
        new Timestamp(0),
        0.0,
        0: Float,
        0: Byte,
        0: Int,
        0L,
        0: Short,
        "a"
      )
      result
    }

    lazy val atomicTypePairs: Seq[(DataType, TypeTag[_])] = atomicExamples.map { v =>
      ScalaType.FromTypeTag(v._2).tryReify.get -> v._2
    }

    lazy val atomicTypeMap: Map[DataType, TypeTag[_]] = {
      Map(atomicTypePairs: _*)
    }
  }

  implicit class FromCatalystType(dataType: DataType) extends ReflectionLock {

    // CatalystType => ScalaType
    // used in ReflectionMixin to determine the exact function to:
    // 1. convert data from CatalystType to canonical Scala Type (and obtain its TypeTag)
    // 2. use the obtained TypeTag to get the specific function implementation and applies to the canonic Scala Type data.
    // 3. get the output TypeTag of the function, use it to generate the output DataType of the new Extraction.
    // TODO: should rely on spark impls including Literal, ScalaReflection & CatalystTypeConverters
    def getTypeTagOpt: Option[TypeTag[_]] = locked {

      dataType match {
        case NullType =>
          Some(TypeTag.Null)
        case st: ScalaType.CatalystTypeMixin[_] =>
          Some(st.self.asTypeTag)
        case t if FromCatalystType.atomicTypeMap.contains(t) =>
          FromCatalystType.atomicTypeMap.get(t)
        case ArrayType(inner, _) =>
          val innerTagOpt = inner.getTypeTagOpt
          innerTagOpt.map {
            case at: TypeTag[a] =>
              implicit val att: TypeTag[a] = at
              typeTag[Array[a]]
          }
        case MapType(key, value, _) =>
          val keyTag = key.getTypeTagOpt
          val valueTag = value.getTypeTagOpt
          val pairs = (keyTag, valueTag) match {
            case (Some(kt), Some(vt)) => Some(kt -> vt)
            case _                    => None
          }

          pairs.map { pair =>
            (pair._1, pair._2) match {
              case (ttg1: TypeTag[a], ttg2: TypeTag[b]) =>
                implicit val t1: TypeTag[a] = ttg1
                implicit val t2: TypeTag[b] = ttg2
                typeTag[Map[a, b]]
            }
          }
        case StructType(fields) =>
          fields.length match {
            case 1 => Some(typeTag[Product1[Any]])
            case 2 => Some(typeTag[Product2[Any, Any]])
            case 3 => Some(typeTag[Product3[Any, Any, Any]])
            case 4 => Some(typeTag[Product4[Any, Any, Any, Any]])
            case 5 => Some(typeTag[Product5[Any, Any, Any, Any, Any]])
            // TODO: keep going for this, OR find more general solution, using shapeless?
            case _ => None
          }
        case _ =>
          None
      }
    }

    @transient lazy val asTypeTag: TypeTag[_] = {
      getTypeTagOpt.getOrElse {
        throw new UnsupportedOperationException(
          s"cannot convert Catalyst type $dataType to Scala type: TypeTag=${dataType.getTypeTagOpt}"
        )
      }
    }

    def asTypeTag_casted[T]: TypeTag[T] = asTypeTag.asInstanceOf[TypeTag[T]]

    @transient lazy val reified: DataType = locked {
      val result = UnreifiedObjectType.reify(dataType)
      result
    }

    @transient lazy val asClassTag: ClassTag[_] = {

      dataType match {
        case v: CatalystTypeMixin[_] =>
          v.self.asClassTag
        case _ =>
          val clazz = org.apache.spark.sql.catalyst.expressions.Literal.default(dataType).eval().getClass
          ClassTag(clazz)
      }
    }

    def asClass: Class[_] = asClassTag.runtimeClass

    def unboxArrayOrMap: DataType = locked {
      dataType._unboxArrayOrMapOpt
        .orElse(
          dataType.reified._unboxArrayOrMapOpt
        )
        .getOrElse(
          throw new UnsupportedOperationException(s"Type $dataType is not an Array")
        )
    }

    private[utils] def _unboxArrayOrMapOpt: Option[DataType] = locked {
      dataType match {
        case ArrayType(boxed, _) =>
          Some(boxed)
        case MapType(keyType, valueType, valueContainsNull) =>
          Some(
            StructType(
              Array(
                StructField("_1", keyType),
                StructField("_2", valueType, valueContainsNull)
              )
            )
          )
        case _ =>
          None
      }
    }

    def filterArray: Option[DataType] = locked {
      if (dataType.reified.isInstanceOf[ArrayType])
        Some(dataType)
      else
        None
    }

    def asArray: DataType = locked {
      filterArray.getOrElse {
        ArrayType(dataType)
      }
    }

    def ensureArray: DataType = locked {
      filterArray.getOrElse {
        throw new UnsupportedOperationException(s"Type $dataType is not an Array")
      }
    }

    def =~=(another: DataType): Boolean = {
      val result = (dataType eq another) ||
        (dataType == another) ||
        (dataType.reified == another.reified)

      result
    }

    def should_=~=(another: DataType): Unit = {
      val result = =~=(another)
      assert(
        result,
        s"""
           |Type not equal:
           |LEFT:  $dataType -> ${dataType.reified}
           |RIGHT: $another -> ${another.reified}
          """.stripMargin
      )
    }
  }

  trait CatalystTypeMixin[T] extends DataType {

    def self: ScalaType[T]
  }
}
