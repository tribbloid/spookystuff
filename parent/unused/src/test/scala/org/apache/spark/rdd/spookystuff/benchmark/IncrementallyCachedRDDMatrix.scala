package org.apache.spark.rdd.spookystuff.benchmark

import com.tribbloids.spookystuff.testutils.FunSpecx
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.execution.UnsafeRowSerializer
import org.apache.spark.storage.StorageLevel

class IncrementallyCachedRDDMatrix extends FunSpecx {

  object Memory extends IncrementallyCachedRDDBenchmark {

    override val storageLevel: StorageLevel = StorageLevel.MEMORY_ONLY
  }

  object MemorySer extends IncrementallyCachedRDDBenchmark {

    override val storageLevel: StorageLevel = StorageLevel.MEMORY_ONLY_SER
  }

  object MemoryAndDisk extends IncrementallyCachedRDDBenchmark {

    override val storageLevel: StorageLevel = StorageLevel.MEMORY_AND_DISK_SER
  }

  class Disk extends IncrementallyCachedRDDBenchmark {}

  object Disk extends Disk {}

  object Disk_UnsafeRows extends Disk {

    override def getRDD: RDD[_] = {

      val count = this.count

      rowSrc.map { v =>
        count.add(1)
        v
      }
    }

    override lazy val serializerFactory: () => UnsafeRowSerializer = () => new UnsafeRowSerializer(4)
  }

  override lazy val nestedSuites: Vector[IncrementallyCachedRDDBenchmark] = Vector(
    Memory,
    MemorySer,
    MemoryAndDisk,
    Disk,
    Disk_UnsafeRows
  )

}
