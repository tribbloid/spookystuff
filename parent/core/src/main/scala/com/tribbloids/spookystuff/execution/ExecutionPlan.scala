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
abstract class ExecutionPlan[D](
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

  protected def execute: SquashedRDD[D]

  final def fetch: SquashedRDD[D] = {

    this.execute
      .map { row =>
        row.group.withCtx(spooky).trajectory // always fetch before rendering an RDD
        row
      }
  }

  @volatile var storageLevel: StorageLevel = StorageLevel.NONE // TODO: this should be in FetchedDataset

  @volatile var _cachedRDD: SquashedRDD[D] = _
  def cachedRDDOpt: Option[SquashedRDD[D]] = Option(_cachedRDD)

  def isCached: Boolean = cachedRDDOpt.nonEmpty

  // TODO: cachedRDD is redundant? just make it lazy val!
  final def squashedRDD: SquashedRDD[D] = {
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

  @transient final lazy val fetchedRDD: RDD[FetchedRow[D]] =
    SquashedRDDWithSchema.flatMap(row => row.withCtx.unSquash)

  def chain_optimised[O](
      fn: ChainPlan.Fn[D, O]
  ): UnaryPlan[D, O] = {

    ChainPlan(this, fn)
//    this match { // TODO: enable this optimisation later
//      case plan: ExplorePlan[_, _] if !this.isCached =>
//        object _More extends Explore.Fn[D, O] {
//
//          override def apply(row: FetchedRow[Data.Exploring[D]]) = {
//
//            val (forked, flat) = plan.fn(row)
//
//            flat.map(withScope => ???)
//            ???
//          }
//        }
//
//        plan.copy()(_More)
//      case _ =>
//        FlatPlan(this, fn)
//    }
  }

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
