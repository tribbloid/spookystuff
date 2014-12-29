package org.tribbloid.spookystuff

import org.apache.spark.SparkContext

import scala.language.implicitConversions

/**
 * Created by peng on 11/7/14.
 */
package object views {

  implicit def mapToItsView[K, V](map: Map[K,V]): MapView[K, V] = new MapView(map)

  implicit def scToItsView(self: SparkContext): SparkContextView = new SparkContextView(self)

  //  def identical[T](vs: TraversableOnce[T]): Boolean = vs.reduce{
  //    (v1,v2) => {
  //      assert(v1 == v2)
  //      v1
  //    }
  //  }
}