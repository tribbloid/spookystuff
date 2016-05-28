package com.tribbloids.spookystuff

import java.util

import scala.collection.mutable

/**
  * Created by peng on 14/06/16.
  */
package object execution {

  import scala.collection.JavaConverters._

  type LinkedMap[K, V] = mutable.LinkedHashMap[K, V]
  def LinkedMap[K, V](): LinkedMap[K, V] = new mutable.LinkedHashMap[K, V]()
}
