package org.tribbloid.spookystuff.utils

import scala.reflect.ClassTag

/**
 * Created by peng on 10/22/14.
 */
@SerialVersionUID(1096592834L)
trait Cacheable extends Serializable

object Cacheable {

  def apply[T: ClassTag](self: T): (T with Cacheable ) = {

    self.asInstanceOf[T with Cacheable]
  }
}
