package org.apache.spark.ml.dsl.utils.refl

import com.tribbloids.spookystuff.utils.CachingUtils.ConcurrentCache

import scala.language.{existentials, implicitConversions}
import scala.reflect.ClassTag

abstract class FromClassMixin {

  val universe: scala.reflect.runtime.universe.type = scala.reflect.runtime.universe
  import universe._

  protected class FromClassTag[T](val _classTag: ClassTag[T]) extends TypeMagnet[T] {

    @transient final override lazy val asClassTag: ClassTag[T] = _classTag

    override lazy val asClass: Class[T] = {
      asClassTag.runtimeClass.asInstanceOf[Class[T]]
    }

    def classLoader: ClassLoader = asClass.getClassLoader

    @transient override lazy val mirror: Mirror = {
      val loader = classLoader
      runtimeMirror(loader)
    }

    @transient override lazy val asType: Type = {
      val classSymbol = getClassSymbol(asClass)
      val tpe = classSymbol.selfType
      tpe
    }

    def getClassSymbol(_class: Class[_]): ClassSymbol = {

      try {
        mirror.classSymbol(_class)
      } catch {
        case _: AssertionError =>
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

  object FromClassTag extends CachedBuilder[ClassTag] {

    override def createNew[T](v: ClassTag[T]): TypeMagnet[T] = new FromClassTag(v)
  }

  trait CachedBuilder[I[_]] extends Serializable {

    protected def createNew[T](v: I[T]): TypeMagnet[T]

    protected lazy val cache: ConcurrentCache[I[_], TypeMagnet[_]] = ConcurrentCache[I[_], TypeMagnet[_]]()

    final def apply[T](
        implicit
        v: I[T]
    ): TypeMagnet[T] = {
      cache
        .getOrElseUpdate(
          v,
          createNew[T](v)
        )
        .asInstanceOf[TypeMagnet[T]]
    }
  }

  object FromClass extends CachedBuilder[Class] {

    override def createNew[T](v: Class[T]): TypeMagnet[T] = new FromClassTag(ClassTag(v))
  }

  // TODO: how to get rid of these boilerplates?
  implicit def _fromClass[T](v: Class[T]): TypeMagnet[T] = FromClass(v)
  implicit def __fromClass[T](
      implicit
      v: Class[T]
  ): TypeMagnet[T] = FromClass(v)
}
