package org.tribbloid.spookystuff.sparkbinding

import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

/**
 * Created by peng on 12/06/14.
 */
class RDDView(val self: RDD[_]) {

  def persistDuring[T <: RDD[_]](newLevel: StorageLevel)(fn: => T): T =
    if (self.getStorageLevel == StorageLevel.NONE){
      self.persist(newLevel)
      val result = fn
      self.unpersist()
      result
    }
    else fn

//  def checkpointNow(): Unit = {
//    persistDuring(StorageLevel.MEMORY_ONLY) {
//      self.checkpoint()
//      self.count()
//      self
//    }
//    Unit
//  }
}
