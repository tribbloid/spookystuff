package org.apache.spark.rdd.spookystuff.IncrementallyCachedRDDBenchmark

import org.apache.spark.storage.StorageLevel

class Memory extends Abstract {

  override val storageLevel: StorageLevel = StorageLevel.MEMORY_ONLY
}
