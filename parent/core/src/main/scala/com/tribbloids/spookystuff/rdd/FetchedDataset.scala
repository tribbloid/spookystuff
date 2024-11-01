package com.tribbloids.spookystuff.rdd

import com.tribbloids.spookystuff.actions._
import com.tribbloids.spookystuff.commons.refl.CatalystTypeOps
import com.tribbloids.spookystuff.conf.SpookyConf
import com.tribbloids.spookystuff.dsl._
import com.tribbloids.spookystuff.execution.ExplorePlan.Params
import com.tribbloids.spookystuff.execution._
import com.tribbloids.spookystuff.row._
import com.tribbloids.spookystuff.SpookyContext
import frameless.{TypedDataset, TypedEncoder}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.storage.StorageLevel

import scala.language.implicitConversions
import scala.reflect.ClassTag

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

    // move to extension, DF support is interfering with Scala 3
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

  object flatMap {

    def apply[O](fn: FlatMapPlan.FlatMap._Fn[D, O]): FetchedDataset[O] = {

      FetchedDataset(
        FlatMapPlan(
          FetchedDataset.this,
          FlatMapPlan.FlatMap.normalise(fn)
        )
      )
    }
  }
  def selectMany: flatMap.type = flatMap

  object map {

    def apply[O](fn: FlatMapPlan.Map._Fn[D, O]): FetchedDataset[O] = {

      FetchedDataset(
        FlatMapPlan(
          FetchedDataset.this,
          FlatMapPlan.Map.normalise(fn)
        )
      )
    }
  }
  def select: map.type = map

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
    *
    * TODO: remove, superceded by "run" function that returns a lifetime object, e.g. `Resource` in cats & kyo
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
// TODO: gone, use map(_.saveContent)
  //  def savePages(
//      path: FetchedRow[D] => String,
//      extension: FetchedRow[D] => Option[String], // set to
//      page: FetchedRow[D] => Doc,
//      overwrite: Boolean = false
//  ): FetchedDataset[D] = {
//
//    val fn: FlatMapPlan.Map._Fn[D, D] = { row: FetchedRow[D] =>
//      val pathStr = path(row)
//      val extStr = extension(row)
//      val doc = page(row)
//
//      row.dataWithScope
//    }
//
//    this.map(
//      fn
//    )
//  }

//  def explode(
//      ex: Extractor[Any],
//      forkType: ForkType = ForkType.default,
//      ordinalField: Field = null,
//      sampler: Sampler[Any] = spooky.conf.flattenSampler
//  ): FetchedDataset = {
//
//    val (on, extracted) = ex match {
//      case Get(ff) =>
//        ff -> this
//      case _ =>
//        val effectiveEx = ex.withForkFieldIfMissing
//        val ff = effectiveEx.field
//        ff -> this.extract(ex)
//    }
//
//    ChainPlan.selectOptimized(extracted.plan, ExplodeData(on, ordinalField, sampler, forkType))
//  }
//
//  // TODO: need to define an API shared between fork and explore for specifying ForkPlan.Fn
//  def fork(
//      on: Extractor[Any], // name is discarded
//      forkType: ForkType = ForkType.default,
//      ordinalField: Field = null, // left & idempotent parameters are missing as they are always set to true
//      sampler: Sampler[Any] = spooky.conf.forkSampler
//  ): FetchedDataset = {
//
//    val result = this
//      .explode(on.withForkFieldIfMissing, forkType, ordinalField, sampler)
//
//    result
//  }

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
  def fetch[O: ClassTag](fn: FetchPlan.Fn[D, O])(
      implicit
      keyBy: Trace => Any = identity,
      genPartitioner: GenPartitioner = spooky.conf.localityPartitioner
  ): FetchedDataset[O] = {

    FetchedDataset(
      FetchPlan(plan, fn, keyBy, genPartitioner)
    )
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
  def explore[O](
      on: ExplorePlan.Fn[D, O]
  )(
      oneToMany: OneToMany = OneToMany.default,
      keyBy: Trace => Any = identity,
      //
      genPartitioner: GenPartitioner = spooky.conf.localityPartitioner,
      range: Range = spooky.conf.exploreRange,
      pathPlanning: PathPlanning = spooky.conf.explorePathPlanning,
      //
      balancingInterval: Int = spooky.conf.exploreBalancingInterval,
      checkpointInterval: Int = spooky.conf.exploreCheckpointInterval, // set to Int.MaxValue to disable checkpointing,
      //
      //      ordinalField: Field = null,
      sampler: Sampler[Any] = spooky.conf.forkSampler
      //      depthField: Field = null, // TODO: Some of them has to be moved upwards
  ): FetchedDataset[O] = {

    val params = Params(range)
    val out: ExplorePlan[D, O] = ExplorePlan(
      plan,
      on,
      keyBy,
      genPartitioner,
      params,
      pathPlanning,
      balancingInterval,
      checkpointInterval
    )
    FetchedDataset[O](
      out: ExecutionPlan[O]
    )
  }
}
