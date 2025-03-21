package com.tribbloids.spookystuff.execution

import com.tribbloids.spookystuff.SpookyContext
import com.tribbloids.spookystuff.row.*
import org.apache.spark.storage.StorageLevel

import scala.reflect.ClassTag

object ExecutionPlan {

  trait CanChain[O] {
    self: ExecutionPlan[O] =>

    def chain[O2: ClassTag](fn: FlatMapPlan.Fn[O, O2]): ExecutionPlan[O2]
  }

  lazy val pprinter = pprint.PPrinter.BlackWhite
}

//right now it vaguely resembles SparkPlan in catalyst
abstract class ExecutionPlan[O](
    val children: Seq[ExecutionPlan[?]],
    val ec: ExecutionContext
) extends Product
    with Serializable {

  import ExecutionPlan.*
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

  object info {

    override lazy val toString: String = pprinter.apply(this).toString()
  }

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

  @transient final private lazy val _prepared: SquashedRDD[O] = {

    ec.tryDeployPlugin()
    // any RDD access will cause all plugins to be deployed

    prepare
  }

  @transient private lazy val _computed = {

    _prepared
      .map { row =>
        row.localityGroup.withCtx(ctx).trajectory
        // always run the agent to get observations before caching RDD
        row
      }
  }

  final def squashedRDD: SquashedRDD[O] = {

    cachedRDDOpt match {
      case Some(cached) =>
        // if cached and loaded, use it
        cached
      case None =>
        _prepared
    }
  }

  /**
    * like prepare, but with execution graph WITH fetched trajectory
    */
  final def computedRDD: SquashedRDD[O] = {

    cachedRDDOpt.foreach { cached =>
      if (cached != _computed) {
        // calling computed if prepared is cached will automatically advance the cache to _computed

        val storageLevel = cached.getStorageLevel
        cached.unpersist()
        cacheInternal(_computed, storageLevel)
      }
    }

    _computed
  }

  @volatile var _cachedRDD: SquashedRDD[O] = _
  def cachedRDDOpt: Option[SquashedRDD[O]] = Option(_cachedRDD)
  // may be prepared or computed, but evaluating computed will replace _cachedRDD with it

  protected def cacheInternal(rdd: SquashedRDD[O], level: StorageLevel): this.type = {

    rdd.persist(level)
    _cachedRDD = rdd
    this
  }

  def persist(storageLevel: StorageLevel = ctx.conf.defaultStorageLevel): this.type = {

    cacheInternal(_prepared, storageLevel)
    this
  }
  def unpersist(blocking: Boolean = true): this.type = {

    cachedRDDOpt.foreach { cached =>
      cached.unpersist(blocking)
    }

    _cachedRDD = null
    this
  }

  def isCached: Boolean = cachedRDDOpt.nonEmpty

  lazy val normalisedPlan: ExecutionPlan[O] = this
}
