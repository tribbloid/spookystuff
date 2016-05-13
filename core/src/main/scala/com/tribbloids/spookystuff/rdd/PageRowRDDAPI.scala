package com.tribbloids.spookystuff.rdd

import com.tribbloids.spookystuff.execution.ExecutionPlan
import com.tribbloids.spookystuff.row.{SquashedPageRow, SquashedRowRDD}
import org.apache.spark.rdd.{RDD, UnionRDD}
import org.apache.spark.storage.StorageLevel

import scala.language.implicitConversions

case class CoalescePlan(
                         child: ExecutionPlan,
                         numPartitions: RDD[_] => Int,
                         shuffle: Boolean = false,
                         ord: Ordering[SquashedPageRow] = null
                       ) extends ExecutionPlan(child) {

  def doExecute(): SquashedRowRDD = {
    val childRDD = child.rdd()
    val n = numPartitions(childRDD)
    childRDD.coalesce(n, shuffle)(ord)
  }
}

case class UnionPlan(
                      override val children: Seq[ExecutionPlan]
                    ) extends ExecutionPlan(children) {

  def doExecute(): SquashedRowRDD = {
    new UnionRDD(
      spooky.sparkContext,
      children.map(_.rdd())
    )
  }
}

/**
  * Created by peng on 2/12/15.
  */
trait PageRowRDDAPI {
  self: PageRowRDD =>

  //  def filter(f: SquashedPageRow => Boolean): PageRowRDD = selfRDD.filter(f)
  //
  //  def distinct(): PageRowRDD = selfRDD.distinct()
  //
  //  def distinct(numPartitions: Int)(implicit ord: Ordering[SquashedPageRow] = null): PageRowRDD =
  //    selfRDD.distinct(numPartitions)(ord)

  protected def _coalesce(
                 numPartitions: RDD[_] => Int = {v => v.getNumPartitions},
                 shuffle: Boolean = false
               )(
                 implicit ord: Ordering[SquashedPageRow] = null
               ): PageRowRDD = this.copy(
    CoalescePlan(plan, numPartitions, shuffle, ord)
  )

  def coalesce(
                numPartitions: Int,
                shuffle: Boolean = false
              )(
                implicit ord: Ordering[SquashedPageRow] = null
              ): PageRowRDD = {

    _coalesce({v => numPartitions}, shuffle)(ord)
  }

  def repartition(
                   numPartitions: Int
                 )(
                   implicit ord: Ordering[SquashedPageRow] = null
                 ): PageRowRDD = {

    coalesce(numPartitions, shuffle = true)(ord)
  }

  //  def sample(withReplacement: Boolean,
  //             fraction: Double,
  //             seed: Long = Utils.random.nextLong()): PageRowRDD =
  //    selfRDD.sample(withReplacement, fraction, seed)

  def union(other: PageRowRDD*): PageRowRDD = this.copy(
    UnionPlan(Seq(plan) ++ other.map(_.plan))
  )

  def ++(other: PageRowRDD): PageRowRDD = this.union(other)

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

  //TODO: advanced caching: persist an Execution Plan and make its deep copies reusable.
  //cache & persist wont' execute plans immediately, they only apply to the result of doExecute() once finished
  def cache(): this.type = persist()

  def persist(): this.type = this.persist(plan.spooky.conf.defaultStorageLevel)

  def persist(newLevel: StorageLevel): this.type = {
    assert(newLevel != StorageLevel.NONE)
    this.storageLevel = newLevel
    this
  }

  def unpersist(blocking: Boolean = true): this.type = {
    this.storageLevel = StorageLevel.NONE
    this.plan.cachedRDD_fetchedOpt.foreach(_._1.unpersist(blocking))
    this.plan.cachedRDD_fetchedOpt = None
    this
  }

  //In contrast, checkpoint is action-like that will doExecute() immediately.
  def checkpoint(): Unit = {
    rdd.checkpoint()
  }

  def isCheckpointed: Boolean = {
    rdd.isCheckpointed
  }

  def getCheckpointFile: Option[String] = {
    rdd.getCheckpointFile
  }
}