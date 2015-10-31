package com.tribbloids.spookystuff.sparkbinding

import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import com.tribbloids.spookystuff.row.PageRow
import com.tribbloids.spookystuff.utils.{Views, Utils}

import scala.language.implicitConversions
import scala.reflect.ClassTag

/**
 * Created by peng on 2/12/15.
 */
trait PageRowRDDApi {
  this: PageRowRDD =>

  import Views._

  private implicit def selfToPageRowRDD(self: RDD[PageRow]): PageRowRDD = this.copy(selfRDD = self)

  def filter(f: PageRow => Boolean): PageRowRDD = selfRDD.filter(f)

  def distinct(): PageRowRDD = selfRDD.distinct()

  def distinct(numPartitions: Int)(implicit ord: Ordering[PageRow] = null): PageRowRDD =
    selfRDD.distinct(numPartitions)(ord)

  def repartition(
                   numPartitions: Int = selfRDD.partitions.length )(
                   implicit ord: Ordering[PageRow] = null
                   ): PageRowRDD =
    selfRDD.repartition(numPartitions)(ord)

  def coalesce(
                numPartitions: Int = selfRDD.partitions.length,
                shuffle: Boolean = false )(
                implicit ord: Ordering[PageRow] = null
                ): PageRowRDD =
    selfRDD.coalesce(numPartitions, shuffle)(ord)

  def sample(withReplacement: Boolean,
             fraction: Double,
             seed: Long = Utils.random.nextLong()): PageRowRDD =
    selfRDD.sample(withReplacement, fraction, seed)

  def union(other: PageRowRDD): PageRowRDD = this.copy(
    selfRDD.union(other.selfRDD),
    this.webCacheRDD.unionByKey(other.webCacheRDD)(_ ++ _),
    this.keys ++ other.keys.toSeq.reverse
  )

  def union(other: RDD[PageRow]): PageRowRDD = selfRDD.union(other)

  def ++(other: RDD[PageRow]): PageRowRDD = this.union(other)

  def sortBy[K](
                 f: (PageRow) => K,
                 ascending: Boolean = true,
                 numPartitions: Int = selfRDD.partitions.length )(
                 implicit ord: Ordering[K], ctag: ClassTag[K]
                 ): PageRowRDD = selfRDD.sortBy(f, ascending, numPartitions)(ord, ctag)

  def intersection(other: PageRowRDD): PageRowRDD = this.copy(
    selfRDD.intersection(other.selfRDD),
    this.webCacheRDD.intersectionByKey(other.webCacheRDD)(_ ++ _),
    this.keys.intersect(other.keys)//TODO: need validation that it won't change sequence
  )

  def intersection(other: RDD[PageRow]): PageRowRDD = selfRDD.intersection(other)

  def intersection(other: PageRowRDD, numPartitions: Int): PageRowRDD = this.copy(
    selfRDD.intersection(other.selfRDD, numPartitions),
    this.webCacheRDD.intersectionByKey(other.webCacheRDD)(_ ++ _),
    this.keys.intersect(other.keys)
  )

  def intersection(other: RDD[PageRow], numPartitions: Int): PageRowRDD = selfRDD.intersection(other, numPartitions)

  def cache(): this.type = {
    selfRDD.cache()
    this
  }

  def persist(): this.type = {
    selfRDD.persist()
    this
  }

  def persist(newLevel: StorageLevel): this.type = {
    selfRDD.persist(newLevel)
    this
  }

  def unpersist(blocking: Boolean = true): this.type = {
    selfRDD.unpersist(blocking)
    this
  }

  def checkpoint() = {
    selfRDD.checkpoint()
  }

  def isCheckpointed: Boolean = {
    selfRDD.isCheckpointed
  }

  def getCheckpointFile: Option[String] = {
    selfRDD.getCheckpointFile
  }
}