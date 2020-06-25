package org.apache.spark.rdd.spookystuf.IncrementallyCachedRDDBenchmark
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.execution.UnsafeRowSerializer

class Disk_UnsafeRows extends Disk {

  override def getRDD: RDD[_] = {

    val count = this.count

    rowSrc.map { v =>
      count.add(1)
      v
    }
  }

  override lazy val serializer = new UnsafeRowSerializer(4)
}
