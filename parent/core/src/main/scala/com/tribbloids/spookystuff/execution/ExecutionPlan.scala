package com.tribbloids.spookystuff.execution

import com.tribbloids.spookystuff.SpookyContext
import com.tribbloids.spookystuff.commons.TreeView
import com.tribbloids.spookystuff.commons.lifespan.Cleanable
import com.tribbloids.spookystuff.row._
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

object ExecutionPlan {}

//right now it vaguely resembles SparkPlan in catalyst
//TODO: may subclass SparkPlan in the future to generate DataFrame directly, but not so fast
abstract class ExecutionPlan[O](
    val children: Seq[ExecutionPlan[_]],
    val ec: SpookyExecutionContext
) extends TreeView.Immutable[ExecutionPlan[_]]
    with Serializable
    with Cleanable {

  def this(
      children: Seq[ExecutionPlan[_]]
  ) = this(
    children,
    children.map(_.ec).reduce(_ :++ _)
  )

  def spooky: SpookyContext = ec.spooky
  def scratchRDDs: ScratchRDDs = ec.scratchRDDs

  protected def computeSchema: SpookySchema = {
    // TODO: merge into outputSchema
    SpookySchema(ec)
  }
  final lazy val outputSchema: SpookySchema = computeSchema

  {
    outputSchema
  }

  def firstChildOpt: Option[ExecutionPlan[_]] = children.headOption

  // beconRDD is always empty, with fixed partitioning, cogroup with it to maximize Local Cache hitting chance
  // by default, inherit from the first child
  final protected def inheritedBeaconRDDOpt: Option[BeaconRDD[LocalityGroup]] =
    firstChildOpt.flatMap(_.beaconRDDOpt)

  lazy val beaconRDDOpt: Option[BeaconRDD[LocalityGroup]] = inheritedBeaconRDDOpt

  protected def execute: SquashedRDD[O]

  final def fetch: SquashedRDD[O] = {

    this.execute
      .map { row =>
        row.group.withCtx(spooky).trajectory // always fetch before rendering an RDD
        row
      }
  }

  @volatile var storageLevel: StorageLevel = StorageLevel.NONE
  @volatile var _cachedRDD: SquashedRDD[O] = _
  def cachedRDDOpt: Option[SquashedRDD[O]] = Option(_cachedRDD)

  def isCached: Boolean = cachedRDDOpt.nonEmpty

  // TODO: cachedRDD is redundant? just make it lazy val!
  final def squashedRDD: SquashedRDD[O] = {
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

  @transient final lazy val SquashedRDDWithSchema = {
    squashedRDD.map(_.withSchema(outputSchema))
  }

  @transient final lazy val fetchedRDD: RDD[FetchedRow[O]] =
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
