package com.tribbloids.spookystuff.utils

import scala.reflect.ClassTag

/**
 * Created by peng on 10/22/14.
 */
//class WithSerializable(final val vid: Long) {
//
//  def withoutSID[T: ClassTag](self: T): (T with Serializable) = {
//
//    self.asInstanceOf[T with Serializable]
//  }
//
//  def apply[T: ClassTag](self: T): (T with Serializable ) = {
//
//    self.asInstanceOf[T @SerialVersionUID(vid) with Serializable]
//  }
//}