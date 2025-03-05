package com.tribbloids.spookystuff.execution

import com.tribbloids.spookystuff.SpookyContext
import com.tribbloids.spookystuff.row.*
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

import scala.reflect.ClassTag

object ExecutionPlan {

  trait CanChain[O] {
    self: ExecutionPlan[O] =>

    def chain[O2: ClassTag](fn: FlatMapPlan.Fn[O, O2]): ExecutionPlan[O2]
  }
}

//right now it vaguely resembles SparkPlan in catalyst
//TODO: may subclass SparkPlan in the future to generate DataFrame directly, but not so fast
abstract class ExecutionPlan[O](
    val children: Seq[ExecutionPlan[?]],
    val ec: ExecutionContext
) extends Serializable {
  // NOT a Cleanable! multiple copies will be created by Ser/De which will cause double free problem

  def this(
      children: Seq[ExecutionPlan[?]]
  ) = this(
    children,
    children.map(_.ec).reduce(_ :++ _)
  )

  def ctx: SpookyContext = ec.ctx
  def tempRefs: TemporaryRefs = ec.tempRefs

  protected def computeSchema: SpookySchema = {
    // TODO: merge into outputSchema
    SpookySchema(ec)
  }
  final lazy val outputSchema: SpookySchema = computeSchema

  {
    outputSchema
  }

  def firstChildOpt: Option[ExecutionPlan[?]] = children.headOption

  // beconRDD is always empty, with fixed partitioning, cogroup with it to maximize Local Cache hitting chance
  // by default, inherit from the first child
  final protected def inheritedBeaconRDDOpt: Option[BeaconRDD[LocalityGroup]] =
    firstChildOpt.flatMap(_.beaconRDDOpt)

  lazy val beaconRDDOpt: Option[BeaconRDD[LocalityGroup]] = inheritedBeaconRDDOpt

  /**
    * @return
    *   RDD with execution graph WITHOUT fetching trajectory
    */
  protected def prepare: SquashedRDD[O]

  /**
    * like prepare, but with execution graph WITH fetched trajectory
    */
  @transient final protected lazy val fetched: SquashedRDD[O] = {

    this.prepare
      .map { row =>
        row.localityGroup.withCtx(ctx).trajectory
        // always run the agent to get observations before caching RDD
        row
      }
  }

  @volatile var storageLevel: StorageLevel = StorageLevel.NONE // TODO: this should be in FetchedDataset

  @volatile var _cachedRDD: SquashedRDD[O] = _
  def cachedRDDOpt: Option[SquashedRDD[O]] = Option(_cachedRDD)

  def isCached: Boolean = cachedRDDOpt.nonEmpty

  def normalise: ExecutionPlan[O] = this

  // TODO: cachedRDD is redundant? just make it lazy val!
  final def squashedRDD: SquashedRDD[O] = {
    ec.tryDeployPlugin()
    // any RDD access will cause all plugins to be deployed

    cachedRDDOpt match {
      case Some(cached) =>
        // if cached and loaded, use it
        cached
      case None =>
        // if not cached, execute from upstream and use it.
        val exe = if (storageLevel != StorageLevel.NONE) {
          _cachedRDD = fetched.persist(storageLevel)
          _cachedRDD
        } else {
          prepare
        }
        exe
    }
  }

  @transient final lazy val SquashedRDDWithSchema = {
    squashedRDD.map(_.withSchema(outputSchema))
  }

  @transient final lazy val rdd: RDD[FetchedRow[O]] = {
    SquashedRDDWithSchema.flatMap(rowWithSchema => rowWithSchema.unSquash)
  }
}
