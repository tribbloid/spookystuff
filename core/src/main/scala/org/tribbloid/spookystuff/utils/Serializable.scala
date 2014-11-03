package org.tribbloid.spookystuff.utils

import scala.reflect.ClassTag

/**
 * Created by peng on 10/22/14.
 */
object Serializable {

  def apply[T: ClassTag](self: T): (T with Serializable) = {

    self.asInstanceOf[T with Serializable]
  }
}