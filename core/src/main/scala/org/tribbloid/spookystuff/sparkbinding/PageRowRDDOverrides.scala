package org.tribbloid.spookystuff.sparkbinding

import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{Partitioner, Partition, TaskContext}
import org.tribbloid.spookystuff.entity.PageRow
import org.tribbloid.spookystuff.utils.Utils

import scala.language.implicitConversions
import scala.reflect.ClassTag

/**
 * Created by peng on 2/12/15.
 */
trait PageRowRDDOverrides extends RDD[PageRow]{
  this: PageRowRDD =>

  import org.tribbloid.spookystuff.views._

  override def getPartitions: Array[Partition] = self.partitions

  override val partitioner: Option[Partitioner] = self.partitioner

  override def compute(split: Partition, context: TaskContext): Iterator[PageRow] =
    self.iterator(split, context)
  //-----------------------------------------------------------------------

  private implicit def selfToPageRowRDD(self: RDD[PageRow]): PageRowRDD = this.copy(self = self)

  override def filter(f: PageRow => Boolean): PageRowRDD = super.filter(f)

  override def distinct(): PageRowRDD = super.distinct()

  override def distinct(numPartitions: Int)(implicit ord: Ordering[PageRow] = null): PageRowRDD =
    super.distinct(numPartitions)(ord)

  override def repartition(
                            numPartitions: Int = partitions.length )(
                            implicit ord: Ordering[PageRow] = null
                            ): PageRowRDD =
    super.repartition(numPartitions)(ord)

  override def coalesce(
                         numPartitions: Int = partitions.length,
                         shuffle: Boolean = false )(
                         implicit ord: Ordering[PageRow] = null
                         ): PageRowRDD =
    super.coalesce(numPartitions, shuffle)(ord)

  override def sample(withReplacement: Boolean,
                      fraction: Double,
                      seed: Long = Utils.random.nextLong()): PageRowRDD =
    super.sample(withReplacement, fraction, seed)

  override def union(other: RDD[PageRow]): PageRowRDD = other match {

    case other: PageRowRDD =>
      this.copy(
        super.union(other.self),
        this.webCache.unionByKey(other.webCache)(_ ++ _),
        this.keys ++ other.keys.toSeq.reverse
      )
    case _ => super.union(other)
  }

  override def ++(other: RDD[PageRow]): PageRowRDD = this.union(other)

  override def sortBy[K](
                          f: (PageRow) => K,
                          ascending: Boolean = true,
                          numPartitions: Int = partitions.length )(
                          implicit ord: Ordering[K], ctag: ClassTag[K]
                          ): PageRowRDD = super.sortBy(f, ascending, numPartitions)(ord, ctag)

  override def intersection(other: RDD[PageRow]): PageRowRDD = other match {

    case other: PageRowRDD =>
      this.copy(
        super.intersection(other.self),
        this.webCache.intersectionByKey(other.webCache)(_ ++ _),
        this.keys.intersect(other.keys)//TODO: need validation that it won't change sequence
      )
    case _ => super.intersection(other)
  }

  override def intersection(other: RDD[PageRow], numPartitions: Int): PageRowRDD = other match {

    case other: PageRowRDD =>
      this.copy(
        super.intersection(other.self),
        this.webCache.intersectionByKey(other.webCache)(_ ++ _),
        this.keys.intersect(other.keys)
      )
    case _ => super.intersection(other, numPartitions)
  }

  override def persist(newLevel: StorageLevel): this.type = {
    self.persist(newLevel)
    this
  }

  override def unpersist(blocking: Boolean = true): this.type = {
    self.unpersist(blocking)
    this
  }
  override def checkpoint() = {
    self.checkpoint()
  }

  override def isCheckpointed: Boolean = {
    self.isCheckpointed
  }

  override def getCheckpointFile: Option[String] = {
    self.getCheckpointFile
  }
}