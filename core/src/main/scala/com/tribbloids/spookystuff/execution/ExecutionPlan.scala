package com.tribbloids.spookystuff.execution

import com.tribbloids.spookystuff._
import com.tribbloids.spookystuff.actions._
import com.tribbloids.spookystuff.expressions._
import com.tribbloids.spookystuff.row._
import com.tribbloids.spookystuff.utils.NOTSerializableMixin
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.trees.TreeNode
import org.apache.spark.storage.StorageLevel

import scala.collection.immutable.ListSet
import scala.collection.mutable.ArrayBuffer
import scala.language.implicitConversions

//right now it vaguely resembles SparkPlan in catalyst
//TODO: may subclass SparkPlan in the future to generate DataFrame directly, but not so fast
abstract class ExecutionPlan(
                              val children: Seq[ExecutionPlan],
                              val schema: ListSet[Field],
                              val spooky: SpookyContext,
                              val cacheQueue: ArrayBuffer[RDD[_]]
                            ) extends TreeNode[ExecutionPlan] with NOTSerializableMixin {

  def firstChildOpt = children.headOption

  //beconRDD is always empty, with fixed partitioning, cogroup with it to maximize In-Memory WebCache hitting chance
  //by default, inherit from the first child
  protected final def defaultLocalityBeaconRDDOpt: Option[RDD[(Trace, DataRow)]] =
    firstChildOpt.flatMap(_.localityBeaconRDDOpt)

  lazy val localityBeaconRDDOpt = defaultLocalityBeaconRDDOpt

  def doExecute(): SquashedFetchedRDD

  final def execute(): SquashedFetchedRDD = {

    this.doExecute()
  }

  var storageLevel: StorageLevel = StorageLevel.NONE
  var cachedRDD_fetchedOpt: Option[(SquashedFetchedRDD, Boolean)] = None

  def isCached = cachedRDD_fetchedOpt.nonEmpty
  def isFetched = cachedRDD_fetchedOpt.exists(_._2)

  //  final def rdd: SquashedRowRDD = rdd(false)

  //support lazy evaluation.
  final def rdd(fetch: Boolean = false): SquashedFetchedRDD = {
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

  def unsquashedRDD: RDD[FetchedRow] = rdd(true)
    .flatMap(v => v.unsquash)

  def this(
            child: ExecutionPlan,
            schemaOpt: Option[ListSet[Field]]
          ) = this(

    Seq(child),
    schemaOpt.getOrElse(child.schema),
    child.spooky,
    child.cacheQueue
  )
  def this(child: ExecutionPlan) = this(child, None)

  def this(
            children: Seq[ExecutionPlan],
            schemaOpt: Option[ListSet[Field]] = None
          ) = this(

    children,
    schemaOpt.getOrElse(children.map(_.schema).reduce(_ ++ _)),
    children.head.spooky,
    children.map(_.cacheQueue).reduce(_ ++ _)
  )
  def this(children: Seq[ExecutionPlan]) = this(children, None)

  @transient def fieldSeq: Seq[Field] = this.schema.toSeq.reverse
  @transient def sortIndexFieldSeq: Seq[Field] = fieldSeq.filter(_.isSortIndex)

  implicit class CacheQueueView(val self: ArrayBuffer[RDD[_]]) {

    def persist[T](
                    rdd: RDD[T],
                    storageLevel: StorageLevel = ExecutionPlan.this.spooky.conf.defaultStorageLevel
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

  final def resolveAlias[T, R](
                                expr: ExpressionLike[T, R],
                                fieldBuffer: ArrayBuffer[Field] = ArrayBuffer.empty
                              ): NamedExpressionLike[T, R] = {

    val result = expr match {
      case a: NamedExpressionLike[_, _] =>
        val resolvedField = a.field.resolveConflict(this.schema)
        expr named resolvedField
      case _ =>
        val fields = this.schema ++ fieldBuffer
        val names = fields.map(_.name)
        val i = (1 to Int.MaxValue).find(
          i =>
            !names.contains("_c" + i)
        ).get
        expr named Field("_c" + i)
    }
    fieldBuffer += result.name
    result
  }

  def batchResolveAlias[T, R](
                               exprs: Seq[ExpressionLike[T, R]]
                             ): Seq[NamedExpressionLike[T, R]] = {
    val buffer = ArrayBuffer.empty[Field]

    val resolvedExprs = exprs.map {
      expr =>
        this.resolveAlias(expr, buffer)
    }
    resolvedExprs
  }

  //TODO: move to PageRowRDD
  //  def agg(exprs: Seq[(PageRow => Any)], reducer: RowReducer): ExecutionPlan = AggPlan(this, exprs, reducer)
  //  def distinctBy(exprs: (PageRow => Any)*): ExecutionPlan = AggPlan(this, exprs, (v1, v2) => v1)
}