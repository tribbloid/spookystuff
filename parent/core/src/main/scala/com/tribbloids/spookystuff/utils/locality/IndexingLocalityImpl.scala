package com.tribbloids.spookystuff.utils.locality

import com.tribbloids.spookystuff.utils.BroadcastWrapper
import org.apache.spark.{Partitioner, TaskContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.util.collection.AppendOnlyMap

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag
import scala.util.Random

object IndexingLocalityImpl {

  case class MapPartitioner(
      numPartitions: Int,
      @transient key2PartitionID: Map[Any, Int]
  ) extends Partitioner {

    val key2PartitionID_broadcast: BroadcastWrapper[Map[Any, Int]] = BroadcastWrapper(key2PartitionID)

    override def getPartition(key: Any): Int = {
      key2PartitionID_broadcast.value.getOrElse(key, Random.nextInt(numPartitions))
    }
  }
}

case class IndexingLocalityImpl[K: ClassTag, V: ClassTag](
    override val rdd1: RDD[(K, V)],
    persistFn: RDD[_] => Unit = _.persist()
) extends LocalityImpl.Ordinality[K, V] {

  import IndexingLocalityImpl._

  val numPartitions1: Int = rdd1.partitions.length

  lazy val beacon: RDD[(Int, (K, V))] = {
    val withPID = rdd1.mapPartitions { itr =>
      val pid = TaskContext.getPartitionId()
      itr.map(v => pid -> v)
    }
    persistFn(withPID)

    withPID
  }

  override def cogroupBase[V2: ClassTag](rdd2: RDD[(K, V2)]): RDD[(K, (V, Iterable[V2]))] = {

    val key2PartitionID: Map[Any, Int] = beacon
      .mapValues(_._1)
      .treeAggregate[mutable.Map[K, Int]](new mutable.HashMap[K, Int]())(
        { (u, tuple) =>
          u += tuple.swap
          u
        },
        { (u1, u2) =>
          u1 ++ u2
        }
      )
      .toMap[Any, Int]

    val partitioner = MapPartitioner(numPartitions1, key2PartitionID)

    val rdd2Repartitioned = rdd2.partitionBy(partitioner)

    val aligned = beacon.zipPartitions(rdd2Repartitioned) { (beaconPartition, partition2) =>
      val partition1 = beaconPartition.map {
        case (k, v) =>
          assert(k == TaskContext.getPartitionId())
          v
      }
      val map = new AppendOnlyMap[K, ArrayBuffer[V2]]()
      // TODO: change to ExternalAppendOnlyMap
      // TODO: change to CompactBuffer
      partition2.foreach { tuple =>
        map.changeValue(
          tuple._1,
          {
            case (hasNull, old) =>
              if (!hasNull) ArrayBuffer(tuple._2)
              else old += tuple._2
          }
        )
      }

      val joinedPartition = partition1.map { tuple =>
        val value2: Iterable[V2] = Option(map.apply(tuple._1)).getOrElse(Iterable.empty)
        tuple._1 -> (tuple._2 -> value2)
      }
      joinedPartition
    }

    val result: RDD[(K, (V, Iterable[V2]))] = aligned

    result
  }
}
