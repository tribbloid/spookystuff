package com.tribbloids.spookystuff.rdd

import ai.acyclic.prover.commons.function.Hom.:=>
import com.tribbloids.spookystuff.actions._
import com.tribbloids.spookystuff.commons.refl.CatalystTypeOps
import com.tribbloids.spookystuff.conf.SpookyConf
import com.tribbloids.spookystuff.doc.Doc
import com.tribbloids.spookystuff.dsl._
import com.tribbloids.spookystuff.execution._
import com.tribbloids.spookystuff.frameless.{Tuple, TypedRow, TypedRowInternal}
import com.tribbloids.spookystuff.row._
import com.tribbloids.spookystuff.utils.SpookyViews
import com.tribbloids.spookystuff.{Const, SpookyContext}
import frameless.{TypedDataset, TypedEncoder}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.storage.StorageLevel

import scala.concurrent.duration.Duration
import scala.language.implicitConversions

object FetchedDataset extends FetchedDatasetImp0 {

  implicit def asRDD[D](self: FetchedDataset[D]): RDD[FetchedRow[D]] = self.rdd
}

/**
  * Created by peng on 8/29/14. Core component, abstraction of distributed Page + schemaless KVStore to represent all
  * stages of remote resource discovery CAUTION: for bug tracking purpose it is important for all RDDs having their
  * names set to their {function name}.{variable names} CAUTION: naming convention: all function ended with _! will be
  * executed immediately, others will yield a logical plan that can be optimized & lazily executed
  */
case class FetchedDataset[D](
    plan: ExecutionPlan[D]
) extends FetchedDatasetAPI[D]
    with CatalystTypeOps.ImplicitMixin {

  import SpookyViews._

  implicit def fromExecutionPlan(plan: ExecutionPlan[D]): FetchedDataset[D] = FetchedDataset(plan)

  def this(
      sourceRDD: SquashedRDD[D],
      spooky: SpookyContext,
      beaconRDDOpt: Option[BeaconRDD[LocalityGroup]] = None
  ) = {

    this(
      RDDPlan(
        SpookySchema(SpookyExecutionContext(spooky)),
        sourceRDD,
        beaconRDDOpt
      )
    )
  }

  // TODO: use reflection for more clear API
  def setConf(f: SpookyConf => Unit): this.type = {
    f(spooky.conf)
    this
  }

  def sparkContext: SparkContext = plan.spooky.sparkContext
  def storageLevel: StorageLevel = plan.storageLevel
  def storageLevel_=(lv: StorageLevel): Unit = {
    plan.storageLevel = lv
  }

  def rdd: RDD[FetchedRow[D]] = this.fetchedRDD

  def spooky: SpookyContext = plan.spooky
  def schema: SpookySchema = plan.outputSchema

  def isCached: Boolean = plan.isCached

//  /** TODO: remove, caching should be handled else
//    * only 1 RDD/Dataset will be cached at a time, if a cached instance with a higher precedence is computed, other
//    * instances with lower precedence will be dropped, its data representation will instead be reconstructed from
//    */
//  trait Cached {}

  trait DataView {
    // this is the only component that can be realistically cached in-memory.
    // all others are too big and already cached in DFS

    def dataRDD: RDD[D]

    final def toDataset(
        implicit
        ev1: TypedEncoder[D]
    ): TypedDataset[D] = {

      val rdd = dataRDD

      val ds = TypedDataset.create(rdd)(ev1, spooky.sparkSession)

      ds
    }

    def toDF(
        implicit
        ev1: TypedEncoder[D]
    ): DataFrame = toDataset.toDF()

//    def toMapRDD(
//        implicit
//        ev1: TypedEncoder[D]
//    ): RDD[Map[String, Any]] = {
//      toDataset. TODO: translate with schema
//    }

    def toJSON(
        implicit
        ev1: TypedEncoder[D]
    ): TypedDataset[String] = {
      toDataset.toJSON
    }
  }

  def chain[O](fn: ChainPlan.Fn[D, O]): FetchedDataset[O] = {

    this.copy(
      plan.chain_optimised(fn)
    )
  }

//  def dataRDDSorted(
//      implicit
//      ev: Ordering[D]
//  ): RDD[D] = { // DO NOT DELETE!
//
//    val dataRDD = this.map(_.data)
//    plan.scratchRDDPersist(dataRDD)
//
//    val sorted = dataRDD.sortBy(identity, ascending = true, 1)
//    sorted.setName("sort")
//
//    sorted.foreachPartition { _ => } // force execution TODO: remove, won't force
//    plan.scratchRDDs.unpersist(dataRDD)
//
//    sorted
//  }

  // IMPORTANT: DO NOT discard type parameter! otherwise arguments' type will be coerced into Any!
//  def select[O](fn: FetchedRow[D] => O): FetchedDataset[O] = { // TODO: add alias
//
//    object _Fn extends NarrowPlan.Select[D, O] {
//
//      override def apply(v1: FetchedRow[D]): Seq[O] = {
//        Seq(fn(v1))
//      }
//    }
//
//    this.copy(
//      NarrowPlan.selectOptimized(plan, _Fn)
//    )
//  }

  // use append the result to the end of TypedRow, need encoder support
  // TODO: should be "withColumns"
//  def extract[
//      IT <: Tuple,
//      O,
//      OT <: Tuple,
//      UT <: Tuple
//  ](fn: FetchedRow[D] => O)(
//      implicit
//      toRow1: TypedRowInternal.ofData.=>>[D, TypedRow[IT]],
//      toRow2: TypedRowInternal.ofData.=>>[O, TypedRow[OT]]
//      // TODO: ++< should be a Poly2 or Hom.Poly, otherwise implicit type declaration will be a nightmare
//  ): FetchedDataset[UT] = {
//
//    object _Fn extends NarrowPlan.Select[D, UT] {
//
//      override def apply(v1: FetchedRow[D]): Seq[UT] = {
//        val out = fn(v1)
//
//        val old = toRow1(v1.data)
//        val neo = toRow2(out)
//
//        Seq(old ++< neo)
//      }
//    }
//
//    this.copy(
//      NarrowPlan.selectOptimized(plan, _Fn)
//    )
//  }

//  def apply[T](exs: Extractor[T]*): FetchedDataset = {
//    DeltaPlan.optimised(plan, Extract(exs))
//  }

//  def explodeObservations(
//      fn: Lineage.WithScope => Seq[Lineage.WithScope]
//  ): FetchedDataset = {
//    NarrowPlan.selectOptimized(plan, ExplodeScope(fn))
//  }

//  def transientRemoved: FetchedDataset = this.remove(fields.filter(_.isTransient): _*)

  /**
    * this is an action that will be triggered immediately
    */
//  def savePages_!(
//      path: Col[String],
//      extension: Col[String] = null,
//      page: Extractor[Doc] = S,
//      overwrite: Boolean = false
//  ): this.type = {
//    val saved = savePages(path, extension, page, overwrite)
//    saved.rdd.forceExecute()
//    this
//  }

  /**
    * save each page to a designated directory support many file systems including but not limited to HDFS, S3 and local
    * HDD
    *
    * @param overwrite
    *   if a file with the same name already exist: true: overwrite it false: append an unique suffix to the new file
    *   name
    */
  // always use the same path pattern for filtered pages, if you want pages to be saved with different path, use multiple saveContent with different names
  def savePages(
      path: Col[String],
      extension: Col[String] = null, // set to
      page: Col[Doc] = S,
      overwrite: Boolean = false
  ): FetchedDataset = {

    val _pageEx: Extractor[Doc] = page.ex.typed[Doc]

    val _extensionEx: Extractor[String] = Option(extension)
      .map(_.ex.typed[String])
      .getOrElse(_pageEx.defaultFileExtension)

    ChainPlan.selectOptimized(
      plan,
      SaveContent(path.ex.typed[String], _extensionEx, _pageEx, overwrite)
    )
  }

  def explode(
      ex: Extractor[Any],
      forkType: ForkType = ForkType.default,
      ordinalField: Field = null,
      sampler: Sampler[Any] = spooky.conf.flattenSampler
  ): FetchedDataset = {

    val (on, extracted) = ex match {
      case Get(ff) =>
        ff -> this
      case _ =>
        val effectiveEx = ex.withForkFieldIfMissing
        val ff = effectiveEx.field
        ff -> this.extract(ex)
    }

    ChainPlan.selectOptimized(extracted.plan, ExplodeData(on, ordinalField, sampler, forkType))
  }

  // TODO: need to define an API shared between fork and explore for specifying ForkPlan.Fn
  def fork(
      on: Extractor[Any], // name is discarded
      forkType: ForkType = ForkType.default,
      ordinalField: Field = null, // left & idempotent parameters are missing as they are always set to true
      sampler: Sampler[Any] = spooky.conf.forkSampler
  ): FetchedDataset = {

    val result = this
      .explode(on.withForkFieldIfMissing, forkType, ordinalField, sampler)

    result
  }

//  protected def _defaultWget(
//      cooldown: Option[Duration] = None,
//      filter: DocFilter = Const.defaultDocumentFilter,
//      on: Col[String] = Get(Const.defaultForkField)
//  ): Trace = {
//
//    val _delay: Trace = getCooldown(cooldown)
//
//    val result = Wget(on, filter) +> _delay
//
//    Trace(result)
//  }

  // Always left
  def fetch(
      traces: HasTraceSet,
      keyBy: Trace => Any = identity,
      genPartitioner: GenPartitioner = spooky.conf.localityPartitioner
  ): FetchedDataset = {

    FetchPlan(plan, traces, keyBy, genPartitioner)
  }

  // shorthand of fetch
//  def wget(
//      on: Col[String],
//      cooldown: Option[Duration] = None,
//      keyBy: Trace => Any = identity,
//      filter: DocFilter = Const.defaultDocumentFilter,
//      failSafe: Int = -1,
//      genPartitioner: GenPartitioner = spooky.conf.localityPartitioner
//  ): FetchedDataset = {
//
//    var trace: Trace = _defaultWget(cooldown, filter, on)
//
//    if (failSafe > 0) trace = Trace.of(ClusterRetry(trace, failSafe))
//
//    this.fetch(
//      trace.asTraceSet,
//      keyBy,
//      genPartitioner = genPartitioner
//    )
//  }

//  def wgetFork(
//      on: Extractor[Any],
//      forkType: ForkType = ForkType.default,
//      ordinalField: Field = null, // left & idempotent parameters are missing as they are always set to true
//      sampler: Sampler[Any] = spooky.conf.forkSampler,
//      cooldown: Option[Duration] = None,
//      keyBy: Trace => Any = identity,
//      filter: DocFilter = Const.defaultDocumentFilter,
//      failSafe: Int = -1,
//      genPartitioner: GenPartitioner = spooky.conf.localityPartitioner
//  ): FetchedDataset = {
//
//    var trace: Trace = _defaultWget(cooldown, filter)
//    if (failSafe > 0) {
//      trace = ClusterRetry(trace, failSafe).trace
//    }
//
//    this
//      .fork(on, forkType, ordinalField, sampler)
//      .fetch(
//        trace.asTraceSet,
//        keyBy,
//        genPartitioner = genPartitioner
//      )
//  }

  // TODO: how to unify this with join?
  def explore(
      on: Extractor[Any],
      forkType: ForkType = ForkType.default,
      ordinalField: Field = null,
      sampler: Sampler[Any] = spooky.conf.forkSampler
  )(
      traces: HasTraceSet,
      keyBy: Trace => Any = identity,
      genPartitioner: GenPartitioner = spooky.conf.localityPartitioner,
      depthField: Field = null, // TODO: Some of them has to be moved upwards
      range: Range = spooky.conf.exploreRange,
      pathPlanning: PathPlanning = spooky.conf.explorePathPlanning,
      epochSize: Int = spooky.conf.exploreEpochSize,
      checkpointInterval: Int = spooky.conf.exploreCheckpointInterval // set to Int.MaxValue to disable checkpointing,
  ): FetchedDataset = {

    val params = Params(depthField, ordinalField, range)

    ExplorePlan(
      plan,
      on.withForkFieldIfMissing,
      sampler,
      forkType,
      traces.asTraceSet.rewriteGlobally(plan.outputSchema),
      keyBy,
      genPartitioner,
      params,
      pathPlanning,
      epochSize,
      checkpointInterval,
      Nil
    )
  }

//  def wgetExplore(
//      on: Extractor[Any],
//      forkType: ForkType = ForkType.default,
//      ordinalField: Field = null,
//      sampler: Sampler[Any] = spooky.conf.forkSampler,
//      filter: DocFilter = Const.defaultDocumentFilter,
//      failSafe: Int = -1,
//      cooldown: Option[Duration] = None,
//      keyBy: Trace => Any = identity,
//      genPartitioner: GenPartitioner = spooky.conf.localityPartitioner,
//      depthField: Field = null,
//      range: Range = spooky.conf.exploreRange,
//      pathPlanning: PathPlanning = spooky.conf.explorePathPlanning,
//      miniBatch: Int = 500,
//      checkpointInterval: Int = spooky.conf.exploreCheckpointInterval // set to Int.MaxValue to disable checkpointing,
//
//  ): FetchedDataset = {
//
//    var trace = _defaultWget(cooldown, filter)
//    if (failSafe > 0) trace = Trace.of(ClusterRetry(trace, failSafe))
//
//    explore(on, forkType, ordinalField, sampler)(
//      trace.asTraceSet,
//      keyBy,
//      genPartitioner,
//      depthField,
//      range,
//      pathPlanning,
//      miniBatch,
//      checkpointInterval
//    )
//  }
}
