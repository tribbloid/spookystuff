package com.tribbloids.spookystuff.conf

import com.tribbloids.spookystuff.utils.Static

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

/**
  * Created by peng on 18/06/17.
  */
case class Submodules[U] private (
    self: mutable.Map[String, U] = mutable.Map.empty
) extends Iterable[U] {

  def getOrBuild[T <: U](implicit ev: Submodules.Builder[T]): T = {

    get(ev)
      .getOrElse {
        val result = ev.default
        self.put(result.getClass.getCanonicalName, result)
        result
      }
  }

  def buildAll[T <: U: ClassTag](): Unit = {
    val ctg = implicitly[ClassTag[T]]
    Submodules.builderRegistry.foreach { builder =>
      if (ctg >:> builder.ctg) {
        this.getOrBuild(builder.asInstanceOf[Submodules.Builder[_ <: T]])
      }
    }
  }

  def get[T <: U](implicit ev: Submodules.Builder[T]): Option[T] = {
    self
      .get(ev.ctg.runtimeClass.getCanonicalName)
      .map {
        _.asInstanceOf[T]
      }
  }

  def transform(f: U => U): Submodules[U] = {

    Submodules(self.values.map(f).toSeq: _*)
  }

  override def iterator: Iterator[U] = self.valuesIterator
}

object Submodules {

  abstract class Builder[T](implicit val ctg: ClassTag[T]) extends Static[T] {

    builderRegistry += this

    def default: T
  }

  def apply[U](
      vs: U*
  ): Submodules[U] = {

    Submodules[U](
      mutable.Map(
        vs.map { v =>
          v.getClass.getCanonicalName -> v
        }: _*
      )
    )
  }

  val builderRegistry: ArrayBuffer[Builder[_]] = ArrayBuffer[Builder[_]]()
}
