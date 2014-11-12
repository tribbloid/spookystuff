package org.tribbloid.spookystuff

import scala.language.implicitConversions

/**
 * Created by peng on 11/7/14.
 */
package object utils {

  implicit def mapToItsFunctions[K](map: Map[K,_]): MapFunctions[K] = new MapFunctions[K](map)

  //  def identical[T](vs: TraversableOnce[T]): Boolean = vs.reduce{
  //    (v1,v2) => {
  //      assert(v1 == v2)
  //      v1
  //    }
  //  }
}
