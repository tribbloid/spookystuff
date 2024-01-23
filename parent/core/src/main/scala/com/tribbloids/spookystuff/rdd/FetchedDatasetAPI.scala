package com.tribbloids.spookystuff.rdd

import com.tribbloids.spookystuff.execution.{CoalescePlan, UnionPlan}
import com.tribbloids.spookystuff.row.SquashedRow
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

/**
  * Created by peng on 2/12/15.
  */
trait FetchedDatasetAPI {
  self: FetchedDataset =>

  //  def filter(f: SquashedPageRow => Boolean): PageRowRDD = selfRDD.filter(f)
  //
  //  def distinct(): PageRowRDD = selfRDD.distinct()
  //
  //  def distinct(numPartitions: Int)(implicit ord: Ordering[SquashedPageRow] = null): PageRowRDD =
  //    selfRDD.distinct(numPartitions)(ord)

  protected def _coalesce(
      numPartitions: RDD[_] => Int = { v =>
        v.partitions.length
      },
      shuffle: Boolean = false
  )(
      implicit
      ord: Ordering[SquashedRow] = null
  ): FetchedDataset = this.copy(
    CoalescePlan(plan, numPartitions, shuffle, ord)
  )

  def coalesce(
      numPartitions: Int,
      shuffle: Boolean = false
  )(
      implicit
      ord: Ordering[SquashedRow] = null
  ): FetchedDataset = {

    _coalesce(
      { _ =>
        numPartitions
      },
      shuffle
    )(ord)
  }

  def repartition(
      numPartitions: Int
  )(
      implicit
      ord: Ordering[SquashedRow] = null
  ): FetchedDataset = {

    coalesce(numPartitions, shuffle = true)(ord)
  }

  //  def sample(withReplacement: Boolean,
  //             fraction: Double,
  //             seed: Long = Utils.random.nextLong()): PageRowRDD =
  //    selfRDD.sample(withReplacement, fraction, seed)

  def union(other: FetchedDataset*): FetchedDataset = this.copy(
    UnionPlan(Seq(plan) ++ other.map(_.plan))
  )

  def ++(other: FetchedDataset): FetchedDataset = this.union(other)

  //  def sortBy[K](
  //                 f: (PageRow) => K,
  //                 ascending: Boolean = true,
  //                 numPartitions: Int = selfRDD.partitions.length )(
  //                 implicit ord: Ordering[K], ctag: ClassTag[K]
  //                 ): PageRowRDD = selfRDD.sortBy(f, ascending, numPartitions)(ord, ctag)
  //
  //  def intersection(other: PageRowRDD): PageRowRDD = this.derive(
  //    selfRDD.intersection(other.selfRDD),
  //    this.webCachePairRDD.intersectionByKey(other.webCachePairRDD)(_ ++ _),
  //    this.schema.intersect(other.schema)//TODO: need validation that it won't change sequence
  //  )
  //
  //  def intersection(other: RDD[SquashedPageRow]): PageRowRDD = selfRDD.intersection(other)
  //
  //  def intersection(other: PageRowRDD, numPartitions: Int): PageRowRDD = this.derive(
  //    selfRDD.intersection(other.selfRDD, numPartitions),
  //    this.webCachePairRDD.intersectionByKey(other.webCachePairRDD)(_ ++ _),
  //    this.schema.intersect(other.schema)
  //  )
  //
  //  def intersection(other: RDD[SquashedPageRow], numPartitions: Int): PageRowRDD = selfRDD.intersection(other, numPartitions)

  // TODO: advanced caching: persist an Execution Plan and make its deep copies reusable.
  // cache & persist wont' execute plans immediately, they only apply to the result of doExecute() once finished
  def cache(): this.type = persist()

  def persist(): this.type = this.persist(plan.spooky.spookyConf.defaultStorageLevel)

  def persist(newLevel: StorageLevel): this.type = {
    assert(newLevel != StorageLevel.NONE)
    this.storageLevel = newLevel
    this
  }

  def unpersist(blocking: Boolean = true): this.type = {
    this.storageLevel = StorageLevel.NONE
    this.plan.cachedRDDOpt.foreach(_.unpersist(blocking))
    this.plan._cachedRDD = null
    this
  }

  // In contrast, checkpoint is action-like that will doExecute() immediately.
  def checkpoint(): Unit = {
    this.squashedRDD.checkpoint()
  }

  def isCheckpointed: Boolean = {
    this.squashedRDD.isCheckpointed
  }

  def getCheckpointFile: Option[String] = {
    this.squashedRDD.getCheckpointFile
  }
}
