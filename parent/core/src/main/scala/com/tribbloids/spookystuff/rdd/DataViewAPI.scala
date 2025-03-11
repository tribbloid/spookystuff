package com.tribbloids.spookystuff.rdd

import com.tribbloids.spookystuff.execution.{CoalescePlan, UnionPlan}
import com.tribbloids.spookystuff.row.SquashedRow
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

/**
  * Created by peng on 2/12/15.
  */
trait DataViewAPI[D] {
  self: DataView[D] =>

  //  def filter(f: SquashedPageRow => Boolean): PageRowRDD = selfRDD.filter(f)
  //
  //  def distinct(): PageRowRDD = selfRDD.distinct()
  //
  //  def distinct(numPartitions: Int)(implicit ord: Ordering[SquashedPageRow] = null): PageRowRDD =
  //    selfRDD.distinct(numPartitions)(ord)

  // TODO: most of the following impl can be reduced to RDDPlan or LogicalPlan

  protected def _coalesce(
      numPartitions: RDD[?] => Int = { v =>
        v.partitions.length
      },
      shuffle: Boolean = false
  )(
      implicit
      ord: Ordering[SquashedRow[D]] = null
  ): DataView[D] = this.copy(
    CoalescePlan(plan, numPartitions, shuffle, ord)
  )

  def coalesce(
      numPartitions: Int,
      shuffle: Boolean = false
  )(
      implicit
      ord: Ordering[SquashedRow[D]] = null
  ): DataView[D] = {

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
      ord: Ordering[SquashedRow[D]] = null
  ): DataView[D] = {

    coalesce(numPartitions, shuffle = true)(ord)
  }

  //  def sample(withReplacement: Boolean,
  //             fraction: Double,
  //             seed: Long = Utils.random.nextLong()): PageRowRDD =
  //    selfRDD.sample(withReplacement, fraction, seed)

  def union(other: DataView[D]*): DataView[D] = this.copy(
    UnionPlan(Seq(plan) ++ other.map(_.plan))
  )

  def ++(other: DataView[D]): DataView[D] = this.union(other)

  // TODO: advanced caching: persist an Execution Plan and make its deep copies reusable.
  // cache & persist wont' execute plans immediately, they only apply to the result of doExecute() once finished
  def cache(): this.type = persist()

  def persist(): this.type = this.persist(plan.ctx.conf.defaultStorageLevel)

  def persist(newLevel: StorageLevel): this.type = {
    assert(newLevel != StorageLevel.NONE)
    plan.persist(newLevel)
    this
  }

  def unpersist(blocking: Boolean = true): this.type = {
    plan.unpersist(blocking)
    plan.cachedRDDOpt.foreach(_.unpersist(blocking))
    plan._cachedRDD = null
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
