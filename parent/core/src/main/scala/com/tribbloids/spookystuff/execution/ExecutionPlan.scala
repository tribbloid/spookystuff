package com.tribbloids.spookystuff.execution

import com.tribbloids.spookystuff.SpookyContext
import com.tribbloids.spookystuff.row._
import com.tribbloids.spookystuff.tree.TreeView
import com.tribbloids.spookystuff.utils.lifespan.Cleanable
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.DataType
import org.apache.spark.storage.StorageLevel

import scala.collection.immutable.ListMap

object ExecutionPlan {}

//right now it vaguely resembles SparkPlan in catalyst
//TODO: may subclass SparkPlan in the future to generate DataFrame directly, but not so fast
abstract class ExecutionPlan(
    val children: Seq[ExecutionPlan],
    val ec: SpookyExecutionContext
) extends TreeView.Immutable[ExecutionPlan]
    with Serializable
    with Cleanable {

  def this(
      children: Seq[ExecutionPlan]
  ) = this(
    children,
    children.map(_.ec).reduce(_ :++ _)
  )

  def spooky: SpookyContext = ec.spooky
  def scratchRDDs: ScratchRDDs = ec.scratchRDDs

  protected def computeSchema: SpookySchema
  final lazy val outputSchema: SpookySchema = computeSchema

  {
    outputSchema
  }

//  implicit def withSchema(row: SquashedRow): SquashedRow.WithSchema = row.withSchema(schema)

  def fieldMap: ListMap[Field, DataType] = outputSchema.fieldTypes

  def allSortIndices: List[IndexedField] = outputSchema.indexedFields.filter(_._1.self.isSortIndex)

  def firstChildOpt: Option[ExecutionPlan] = children.headOption

  // beconRDD is always empty, with fixed partitioning, cogroup with it to maximize Local Cache hitting chance
  // by default, inherit from the first child
  final protected def inheritedBeaconRDDOpt: Option[BeaconRDD[LocalityGroup]] =
    firstChildOpt.flatMap(_.beaconRDDOpt)

  lazy val beaconRDDOpt: Option[BeaconRDD[LocalityGroup]] = inheritedBeaconRDDOpt

  protected def execute: SquashedRDD

  final def fetch: SquashedRDD = {

    this.execute
      .map { row =>
        row.group.withCtx(spooky).trajectory // always fetch before rendering an RDD
        row
      }
  }

  @volatile var storageLevel: StorageLevel = StorageLevel.NONE
  @volatile var _cachedRDD: SquashedRDD = _
  def cachedRDDOpt: Option[SquashedRDD] = Option(_cachedRDD)

  def isCached: Boolean = cachedRDDOpt.nonEmpty

  // TODO: cachedRDD is redundant? just make it lazy val!
  final def squashedRDD: SquashedRDD = {
    ec.tryDeployPlugin()
    // any RDD access will cause all plugins to be deployed

    cachedRDDOpt match {
      // if cached and loaded, use it
      case Some(cached) =>
        cached
      // if not cached, execute from upstream and use it.
      case None =>
        val exe = fetch
        val result = exe

        if (storageLevel != StorageLevel.NONE) {
          _cachedRDD = result.persist(storageLevel)
        }
        result
    }
  }

  @transient final lazy val SquashedRDDWithSchema: SquashedRDDWithSchema = {
    squashedRDD.map(_.withSchema(outputSchema))
  }

  @transient final lazy val fetchedRDD: RDD[FetchedRow] =
    SquashedRDDWithSchema.flatMap(row => row.withCtx.unSquash)

  // -------------------------------------

  def scratchRDDPersist[T](
      rdd: RDD[T],
      storageLevel: StorageLevel = ExecutionPlan.this.spooky.conf.defaultStorageLevel
  ): RDD[T] = scratchRDDs.persist(rdd, storageLevel)

  override protected def cleanImpl(): Unit = {
//    cachedRDDOpt.foreach { v => // TODO: fix lifespan
//      v.unpersist(false)
//    }
  }
}
