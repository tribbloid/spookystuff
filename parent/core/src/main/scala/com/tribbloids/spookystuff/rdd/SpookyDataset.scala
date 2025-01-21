package com.tribbloids.spookystuff.rdd

import com.tribbloids.spookystuff.SpookyContext
import com.tribbloids.spookystuff.actions.*
import com.tribbloids.spookystuff.commons.refl.CatalystTypeOps
import com.tribbloids.spookystuff.conf.SpookyConf
import com.tribbloids.spookystuff.dsl.*
import com.tribbloids.spookystuff.execution.*
import com.tribbloids.spookystuff.execution.ExplorePlan.Params
import com.tribbloids.spookystuff.rdd.SpookyDataset.DataView
import com.tribbloids.spookystuff.row.*
import frameless.{TypedDataset, TypedEncoder}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.storage.StorageLevel

import scala.language.implicitConversions
import scala.reflect.ClassTag

object SpookyDataset extends SpookyDataset_Imp0 {

  implicit def asRDD[D](self: SpookyDataset[D]): RDD[FetchedRow[D]] = self.rdd

  class DataView[D: ClassTag](
      val spark: SparkSession,
      val toRDD: RDD[D]
  ) {

    def sortBy[T: ClassTag: Ordering](fn: D => T, ascending: Boolean = true): DataView[D] = {
      val sorted = toRDD.sortBy(fn, ascending)
      new DataView(spark, sorted)
    }

    def sorted(ascending: Boolean = true)(
        implicit
        lemma: Ordering[D]
    ): DataView[D] = {

      sortBy[D](identity, ascending)
    }
  }

  implicit class FramelessView[D](
      ops: DataView[D]
  )(
      implicit
      enc: TypedEncoder[D]
  ) {

    final def toFrameless: TypedDataset[D] = {

      val ds = TypedDataset.create(ops.toRDD)(enc, ops.spark)

      ds
    }

    final def toDataset: Dataset[D] = toFrameless.dataset

    // move to extension, DF support is interfering with Scala 3
    def toDF: DataFrame = toDataset.toDF()

    def toJSON: Dataset[String] = {
      toDataset.toJSON
    }

  }

  def ofRDD[D](
      rdd: SquashedRDD[D],
      spooky: SpookyContext,
      beaconRDDOpt: Option[BeaconRDD[LocalityGroup]] = None
  ): SpookyDataset[D] = {

    SpookyDataset(
      RDDPlan(
        SpookySchema(ExecutionContext(spooky)),
        rdd,
        beaconRDDOpt
      )
    )
  }
}

/**
  * Created by peng on 8/29/14. Core component, abstraction of distributed Page + schemaless KVStore to represent all
  * stages of remote resource discovery CAUTION: for bug tracking purpose it is important for all RDDs having their
  * names set to their {function name}.{variable names} CAUTION: naming convention: all function ended with _! will be
  * executed immediately, others will yield a logical plan that can be optimized & lazily executed
  */
case class SpookyDataset[D](
    plan: ExecutionPlan[D]
) extends DatasetAPI[D]
    with CatalystTypeOps.ImplicitMixin {
  // TODO: should be "ExecutionPlanView"

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

  def data(
      implicit
      enc: ClassTag[D]
  ): DataView[D] = {

    new DataView(spooky.sparkSession, rdd.map(_.data))
  }

//  /** TODO: remove, caching should be handled else
//    * only 1 RDD/Dataset will be cached at a time, if a cached instance with a higher precedence is computed, other
//    * instances with lower precedence will be dropped, its data representation will instead be reconstructed from
//    */
//  trait Cached {}

  def execute(): this.type = {
    rdd.foreach(_ => ())
    this
  }

  object flatMap {

    def apply[O: ClassTag](
        fn: ChainPlan.FlatMap._Fn[D, O]
    ): SpookyDataset[O] = {

      SpookyDataset(
        ChainPlan(
          SpookyDataset.this,
          ChainPlan.FlatMap.normalise(fn)
        )
      )
    }
  }
  def selectMany: flatMap.type = flatMap

  object map {

    def apply[O: ClassTag](fn: ChainPlan.Map._Fn[D, O]): SpookyDataset[O] = {

      SpookyDataset(
        ChainPlan(
          SpookyDataset.this,
          ChainPlan.Map.normalise(fn)
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

  // Always left
  def fetch(fn: FetchPlan.Invar._Fn[D])(
      implicit
      keyBy: Trace => Any = identity,
      genPartitioner: GenPartitioner = spooky.conf.localityPartitioner,
      ctg: ClassTag[D]
  ): SpookyDataset[D] = {

    SpookyDataset(
      FetchPlan(plan, FetchPlan.Invar.normalise(fn), keyBy, genPartitioner)
    )
  }

  case class inductively(
      range: Range = spooky.conf.exploreRange,
      pathPlanning: PathPlanning = spooky.conf.explorePathPlanning,
      //
      epochInterval: Int = spooky.conf.exploreEpochInterval,
      checkpointInterval: Int = spooky.conf.exploreCheckpointInterval // set to Int.MaxValue to disable checkpointing,
      //
      //      ordinalField: Field = null,
      //      depthField: Field = null, // TODO: Some of them has to be moved upwards
  )(
      implicit
      ctg: ClassTag[D]
  ) {

    def fetch(fn: ExplorePlan.Invar._Fn[D])(
        implicit
        keyBy: Trace => Any = identity,
        genPartitioner: GenPartitioner = spooky.conf.localityPartitioner
    ): SpookyDataset[Data.Exploring[D]] = {
      val actualFn = ExplorePlan.Invar.normalise(fn)

      val params = Params(range)
      val out: ExplorePlan[D, Data.Exploring[D]] = ExplorePlan(
        plan,
        actualFn,
        keyBy,
        genPartitioner,
        params,
        pathPlanning,
        epochInterval,
        checkpointInterval
      )
      SpookyDataset(
        out
      )
    }

//    object flatMap {}
// TODO: unnecessary, normalisation took over
//    object map {}
  }

  // TODO: how to unify this with join?
//  def explore[O](
//      fn: ExplorePlan.Fn[D, O]
//  )(
//      sampler: Sampler = spooky.conf.exploreSampler,
//      keyBy: Trace => Any = identity,
//      //
//      genPartitioner: GenPartitioner = spooky.conf.localityPartitioner,
//      range: Range = spooky.conf.exploreRange,
//      pathPlanning: PathPlanning = spooky.conf.explorePathPlanning,
//      //
//      epochInterval: Int = spooky.conf.exploreEpochInterval,
//      checkpointInterval: Int = spooky.conf.exploreCheckpointInterval // set to Int.MaxValue to disable checkpointing,
//      //
//      //      ordinalField: Field = null,
//      //      depthField: Field = null, // TODO: Some of them has to be moved upwards
//  ): FetchedDataset[O] = {
//
//    val params = Params(range)
//    val out: ExplorePlan[D, O] = ExplorePlan(
//      plan,
//      fn,
//      keyBy,
//      genPartitioner,
//      params,
//      pathPlanning,
//      epochInterval,
//      checkpointInterval
//    )
//    FetchedDataset[O](
//      out: ExecutionPlan[O]
//    )
//  }
}
