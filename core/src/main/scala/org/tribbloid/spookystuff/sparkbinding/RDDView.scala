package org.tribbloid.spookystuff.sparkbinding

import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

import scala.collection.immutable.ListMap

/**
 * Created by peng on 12/06/14.
 */
class RDDView(val self: RDD[_]) {

  def persistDuring[T <: RDD[_]](newLevel: StorageLevel)(fn: => T): T =
    if (self.getStorageLevel == StorageLevel.NONE){
      self.persist(newLevel)
      val result = fn
      result.count()
      self.unpersist()//TODO: what's the point of block argument?
      result
    }
    else fn

  def checkpointNow(): Unit = {
    persistDuring(StorageLevel.MEMORY_ONLY) {
      self.checkpoint()
      self
    }
    Unit
  }
}
