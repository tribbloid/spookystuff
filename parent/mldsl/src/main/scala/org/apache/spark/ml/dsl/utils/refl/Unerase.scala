package org.apache.spark.ml.dsl.utils.refl

import scala.reflect.runtime.universe
import scala.collection.concurrent.TrieMap
import scala.language.implicitConversions

case class Unerase[T](self: T)(
    implicit
    ev: universe.TypeTag[T]
) {

  import Unerase._

  cache += {

    val inMemoryId = System.identityHashCode(this)
    inMemoryId -> ev
  }
}

object Unerase {

  lazy val cache = TrieMap.empty[Int, universe.TypeTag[_]]

  def get[T](v: T): Option[universe.TypeTag[T]] = {
    val inMemoryId = System.identityHashCode(v)
    cache.get(inMemoryId).map { tt =>
      tt.asInstanceOf[universe.TypeTag[T]]
    }
  }

  implicit def unbox[T](v: Unerase[T]): T = v.self

  implicit def box[T](v: T)(
      implicit
      ev: universe.TypeTag[T]
  ): Unerase[T] = Unerase(v)
}
