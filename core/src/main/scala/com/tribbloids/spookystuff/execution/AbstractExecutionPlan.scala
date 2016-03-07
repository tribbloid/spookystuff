package com.tribbloids.spookystuff.execution

import com.tribbloids.spookystuff._
import com.tribbloids.spookystuff.actions._
import com.tribbloids.spookystuff.row._
import com.tribbloids.spookystuff.utils.NOTSerializableMixin
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.trees.TreeNode
import org.apache.spark.storage.StorageLevel

import scala.collection.immutable.ListSet
import scala.collection.mutable.ArrayBuffer
import scala.language.implicitConversions

//right now it vaguely resembles SparkPlan in catalyst
abstract class AbstractExecutionPlan(
                                      val children: Seq[AbstractExecutionPlan],
                                      val fieldSet: ListSet[Field],
                                      val spooky: SpookyContext,
                                      val cacheQueue: ArrayBuffer[RDD[_]]
                                    ) extends TreeNode[AbstractExecutionPlan] with NOTSerializableMixin {

  def firstChildOpt = children.headOption

  //beconRDD is always empty, with fixed partitioning, cogroup with it to maximize In-Memory WebCache hitting chance
  //by default, inherit from the first child
  protected final def defaultLocalityBeaconRDDOpt: Option[RDD[(Trace, DataRow)]] =
    firstChildOpt.flatMap(_.localityBeaconRDDOpt)

  lazy val localityBeaconRDDOpt = defaultLocalityBeaconRDDOpt

  def doExecute(): SquashedRowRDD

  final def execute(): SquashedRowRDD = {

    this.doExecute()
  }

  var storageLevel: StorageLevel = StorageLevel.NONE
  var cachedRDD_fetchedOpt: Option[(SquashedRowRDD, Boolean)] = None

  def isCached = cachedRDD_fetchedOpt.nonEmpty
  def isFetched = cachedRDD_fetchedOpt.exists(_._2)

  //  final def rdd: SquashedRowRDD = rdd(false)

  //support lazy evaluation.
  final def rdd(fetch: Boolean = false): SquashedRowRDD = {
    cachedRDD_fetchedOpt match {
      case Some((cached, true)) =>
        cached
      case Some((cached, false)) =>
        if (!fetch) cached
        else {
          val result = cached.map(_.fetch(spooky))
          if (storageLevel != StorageLevel.NONE) {
            cachedRDD_fetchedOpt = Some((result.persist(storageLevel), true))
          }
          result.count()
          cached.unpersist()
          result
        }
      case None =>
        val exe = execute()
        val result = if (!fetch) exe
        else exe.map(_.fetch(spooky))

        if (storageLevel != StorageLevel.NONE) {
          cachedRDD_fetchedOpt = Some(result.persist(storageLevel), fetch)
        }
        result
    }
  }

  def unsquashedRDD: RDD[PageRow] = rdd(true)
    .flatMap(v => v.unsquash)

  def this(
            child: AbstractExecutionPlan,
            schemaOpt: Option[ListSet[Field]]
          ) = this(

    Seq(child),
    schemaOpt.getOrElse(child.fieldSet),
    child.spooky,
    child.cacheQueue
  )
  def this(child: AbstractExecutionPlan) = this(child, None)

  def this(
            children: Seq[AbstractExecutionPlan],
            schemaOpt: Option[ListSet[Field]] = None
          ) = this(

    children,
    schemaOpt.getOrElse(children.map(_.fieldSet).reduce(_ ++ _)),
    children.head.spooky,
    children.map(_.cacheQueue).reduce(_ ++ _)
  )
  def this(children: Seq[AbstractExecutionPlan]) = this(children, None)

  @transient def fieldSeq: Seq[Field] = this.fieldSet.toSeq.reverse
  @transient def sortIndexFieldSeq: Seq[Field] = fieldSeq.filter(_.isSortIndex)

  implicit class CacheQueueView(val self: ArrayBuffer[RDD[_]]) {

    def persist[T](
                    rdd: RDD[T],
                    storageLevel: StorageLevel = AbstractExecutionPlan.this.spooky.conf.defaultStorageLevel
                  ): RDD[T] = {
      if (rdd.getStorageLevel == StorageLevel.NONE) {
        self += rdd.persist(storageLevel)
      }
      rdd
    }

    def unpersist[T](
                      rdd: RDD[T],
                      blocking: Boolean = true
                    ): RDD[T] = {
      rdd.unpersist(blocking)
      self -= rdd
      rdd
    }

    def unpersistAll(
                      except: Set[RDD[_]] = Set(),
                      blocking: Boolean = true
                    ): Unit = {
      val unpersisted = self.filter(except.contains)
        .map {
          _.unpersist(blocking)
        }

      self --= unpersisted
    }
  }

  //TODO: move to PageRowRDD
  def agg(exprs: Seq[(PageRow => Any)], reducer: RowReducer): AbstractExecutionPlan = AggPlan(this, exprs, reducer)
  def distinctBy(exprs: (PageRow => Any)*): AbstractExecutionPlan = AggPlan(this, exprs, (v1, v2) => v1)
}