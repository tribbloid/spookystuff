package com.tribbloids.spookystuff.commons.refl

import scala.collection.concurrent.TrieMap
import scala.language.implicitConversions
import scala.reflect.runtime.universe

case class Unerase[T](self: T)(
    implicit
    ev: universe.TypeTag[T]
) {

  import Unerase.*

  cache += {

    val inMemoryId = System.identityHashCode(this)
    inMemoryId -> ev
  }
}

object Unerase {

  lazy val cache: TrieMap[Int, universe.TypeTag[?]] = TrieMap.empty[Int, universe.TypeTag[?]]

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
