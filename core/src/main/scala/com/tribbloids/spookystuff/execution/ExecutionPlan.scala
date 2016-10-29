package com.tribbloids.spookystuff.execution

import com.tribbloids.spookystuff._
import com.tribbloids.spookystuff.actions.TraceView
import com.tribbloids.spookystuff.row._
import com.tribbloids.spookystuff.utils.NOTSerializable
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.trees.TreeNode
import org.apache.spark.sql.types.DataType
import org.apache.spark.storage.StorageLevel

import scala.collection.immutable.ListMap
import scala.collection.mutable.ArrayBuffer
import scala.language.implicitConversions

//right now it vaguely resembles SparkPlan in catalyst
//TODO: may subclass SparkPlan in the future to generate DataFrame directly, but not so fast
abstract class ExecutionPlan(
                              val children: Seq[ExecutionPlan],
                              val spooky: SpookyContext,
                              val cacheQueue: ArrayBuffer[RDD[_]]
                            ) extends TreeNode[ExecutionPlan] with NOTSerializable {

  def this(
            children: Seq[ExecutionPlan]
          ) = this(

    children,
    children.head.spooky,
    children.map(_.cacheQueue).reduce(_ ++ _)
  )

  //Cannot be lazy, always defined on construction
  val schema: DataRowSchema = DataRowSchema(
    spooky,
    map = children.map(_.schema.map)
      .reduceOption(_ ++ _)
      .getOrElse(ListMap[Field, DataType]())
  )

  implicit def withSchema(row: SquashedFetchedRow): SquashedFetchedRow#WithSchema = new row.WithSchema(schema)

  def fieldMap: ListMap[Field, DataType] = schema.map

  def allSortIndices: List[IndexedField] = schema.indexedFields.filter(_._1.self.isSortIndex)

  def firstChildOpt = children.headOption

  //beconRDD is always empty, with fixed partitioning, cogroup with it to maximize Local Cache hitting chance
  //by default, inherit from the first child
  protected final def defaultBeaconRDDOpt =
    firstChildOpt.flatMap(_.beaconRDDOpt)

  lazy val beaconRDDOpt: Option[RDD[(TraceView, DataRow)]] = defaultBeaconRDDOpt

  def doExecute(): SquashedFetchedRDD

  final def execute(): SquashedFetchedRDD = {

    this.doExecute()
  }

  var storageLevel: StorageLevel = StorageLevel.NONE
  var cachedRDD: Option[SquashedFetchedRDD] = None

  def isCached = cachedRDD.nonEmpty

  //support lazy evaluation.
  final def rdd(): SquashedFetchedRDD = {
    cachedRDD match {
      // if cached and loaded, use it
      case Some(cached) =>
        cached
      // if not cached, execute from upstream and use it.
      case None =>
        val exe = execute()
        val result = exe

        if (storageLevel != StorageLevel.NONE) {
          cachedRDD = Some(result.persist(storageLevel))
        }
        result
    }
  }

  def unsquashedRDD: RDD[FetchedRow] = rdd()
    .flatMap(v => new v.WithSchema(schema).unsquash)

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
}

abstract class UnaryPlan(
                          val child: ExecutionPlan,
                          override val spooky: SpookyContext,
                          override val cacheQueue: ArrayBuffer[RDD[_]]
                        ) extends ExecutionPlan(Seq(child), spooky, cacheQueue) {

  def this(
            child: ExecutionPlan
          ) = this(
    child,
    child.spooky,
    child.cacheQueue
  )
}