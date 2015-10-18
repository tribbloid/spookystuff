package com.tribbloids.spookystuff.sparkbinding

import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import com.tribbloids.spookystuff.entity.PageRow
import com.tribbloids.spookystuff.utils.Utils

import scala.language.implicitConversions
import scala.reflect.ClassTag

/**
 * Created by peng on 2/12/15.
 */
trait PageRowRDDApi {
  this: PageRowRDD =>

  import com.tribbloids.spookystuff.Views._

  private implicit def selfToPageRowRDD(self: RDD[PageRow]): PageRowRDD = this.copy(self = self)

  def filter(f: PageRow => Boolean): PageRowRDD = self.filter(f)

  def distinct(): PageRowRDD = self.distinct()

  def distinct(numPartitions: Int)(implicit ord: Ordering[PageRow] = null): PageRowRDD =
    self.distinct(numPartitions)(ord)

  def repartition(
                   numPartitions: Int = self.partitions.length )(
                   implicit ord: Ordering[PageRow] = null
                   ): PageRowRDD =
    self.repartition(numPartitions)(ord)

  def coalesce(
                numPartitions: Int = self.partitions.length,
                shuffle: Boolean = false )(
                implicit ord: Ordering[PageRow] = null
                ): PageRowRDD =
    self.coalesce(numPartitions, shuffle)(ord)

  def sample(withReplacement: Boolean,
             fraction: Double,
             seed: Long = Utils.random.nextLong()): PageRowRDD =
    self.sample(withReplacement, fraction, seed)

  def union(other: PageRowRDD): PageRowRDD = this.copy(
    self.union(other.self),
    this.webCacheRDD.unionByKey(other.webCacheRDD)(_ ++ _),
    this.keys ++ other.keys.toSeq.reverse
  )

  def union(other: RDD[PageRow]): PageRowRDD = self.union(other)

  def ++(other: RDD[PageRow]): PageRowRDD = this.union(other)

  def sortBy[K](
                 f: (PageRow) => K,
                 ascending: Boolean = true,
                 numPartitions: Int = self.partitions.length )(
                 implicit ord: Ordering[K], ctag: ClassTag[K]
                 ): PageRowRDD = self.sortBy(f, ascending, numPartitions)(ord, ctag)

  def intersection(other: PageRowRDD): PageRowRDD = this.copy(
    self.intersection(other.self),
    this.webCacheRDD.intersectionByKey(other.webCacheRDD)(_ ++ _),
    this.keys.intersect(other.keys)//TODO: need validation that it won't change sequence
  )

  def intersection(other: RDD[PageRow]): PageRowRDD = self.intersection(other)

  def intersection(other: PageRowRDD, numPartitions: Int): PageRowRDD = this.copy(
    self.intersection(other.self, numPartitions),
    this.webCacheRDD.intersectionByKey(other.webCacheRDD)(_ ++ _),
    this.keys.intersect(other.keys)
  )

  def intersection(other: RDD[PageRow], numPartitions: Int): PageRowRDD = self.intersection(other, numPartitions)

  def cache(): this.type = {
    self.cache()
    this
  }

  def persist(): this.type = {
    self.persist()
    this
  }

  def persist(newLevel: StorageLevel): this.type = {
    self.persist(newLevel)
    this
  }

  def unpersist(blocking: Boolean = true): this.type = {
    self.unpersist(blocking)
    this
  }

  def checkpoint() = {
    self.checkpoint()
  }

  def isCheckpointed: Boolean = {
    self.isCheckpointed
  }

  def getCheckpointFile: Option[String] = {
    self.getCheckpointFile
  }
}