package com.tribbloids.spookystuff.rdd

import com.tribbloids.spookystuff.SpookyContext
import com.tribbloids.spookystuff.actions.*
import com.tribbloids.spookystuff.commons.refl.CatalystTypeOps
import com.tribbloids.spookystuff.conf.SpookyConf
import com.tribbloids.spookystuff.dsl.*
import com.tribbloids.spookystuff.execution.*
import com.tribbloids.spookystuff.execution.ExplorePlan.Params
import com.tribbloids.spookystuff.rdd.FetchedDataset.DataView
import com.tribbloids.spookystuff.row.*
import frameless.{TypedDataset, TypedEncoder}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.storage.StorageLevel

import scala.language.implicitConversions
import scala.reflect.ClassTag

object FetchedDataset extends FetchedDatasetImp0 {

  implicit def asRDD[D](self: FetchedDataset[D]): RDD[FetchedRow[D]] = self.rdd

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

    implicit private def ctg: ClassTag[D] = enc.classTag

    final def toFrameless: TypedDataset[D] = {

      val ds = TypedDataset.create(ops.toRDD)(enc, ops.spark)

      ds
    }

    final def toDataset: Dataset[D] = toFrameless.dataset

    // move to extension, DF support is interfering with Scala 3
    def toDF: DataFrame = toDataset.toDF()

    //    def toMapRDD(
    //        implicit
    //        ev1: TypedEncoder[D]
    //    ): RDD[Map[String, Any]] = {
    //      toDataset. TODO: translate with schema
    //    }

    def toJSON: Dataset[String] = {
      toDataset.toJSON
    }

  }
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

//  implicit def fromExecutionPlan(plan: ExecutionPlan[D]): FetchedDataset[D] = FetchedDataset(plan)

  def this(
      sourceRDD: SquashedRDD[D],
      spooky: SpookyContext,
      beaconRDDOpt: Option[BeaconRDD[LocalityGroup]] = None
  ) = {

    this(
      RDDPlan(
        SpookySchema(ExecutionContext(spooky)),
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
  def fetch(fn: FetchPlan.TraceOnly._Fn[D])(
      implicit
      keyBy: Trace => Any = identity,
      genPartitioner: GenPartitioner = spooky.conf.localityPartitioner,
      ctg: ClassTag[D]
  ): FetchedDataset[D] = {

    FetchedDataset(
      FetchPlan(plan, FetchPlan.TraceOnly.normalise(fn), keyBy, genPartitioner)
    )
  }

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
