package com.tribbloids.spookystuff.rdd

import ai.acyclic.prover.commons.util.Magnet.OptionMagnet
import com.tribbloids.spookystuff.SpookyContext
import com.tribbloids.spookystuff.actions.*
import com.tribbloids.spookystuff.commons.refl.CatalystTypeOps
import com.tribbloids.spookystuff.conf.SpookyConf
import com.tribbloids.spookystuff.dsl.*
import com.tribbloids.spookystuff.execution.*
import com.tribbloids.spookystuff.execution.ExplorePlan.Params
import com.tribbloids.spookystuff.row.*
import frameless.{TypedDataset, TypedEncoder}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.storage.StorageLevel

import scala.language.implicitConversions
import scala.reflect.ClassTag

object SpookyDataset extends SpookyDataset_Imp0 {

  implicit def asRDD[D](self: SpookyDataset[D]): RDD[FetchedRow[D]] = self.rdd

//  class ExportDataView[D: ClassTag](
//      val spark: SparkSession,
//      val toRDD: RDD[D]
//  ) {}

  implicit class TypedDatasetView[D](
      ops: SpookyDataset[D]
  )(
      implicit
      ctag: ClassTag[D],
      enc: TypedEncoder[D]
  ) {

    final def toFrameless: TypedDataset[D] = {

      val ds = TypedDataset.create(ops.data)(enc, ops.spooky.sparkSession)

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

  def spooky: SpookyContext = plan.spooky
  def schema: SpookySchema = plan.outputSchema

  def isCached: Boolean = plan.isCached

  def data(
      implicit
      ctag: ClassTag[D]
  ): RDD[D] = {
    this.rdd.map(_.data)
  }

  def sortBy[E: ClassTag: Ordering](
      fn: SortPlan.Fn[D, E],
      ascending: Boolean = true,
      numPartitions: OptionMagnet[Int] = None
  ): SpookyDataset[D] = {
    val plan = SortPlan(this.plan, fn, ascending, numPartitions)

    SpookyDataset(plan)
  }

  def sorted(
      ascending: Boolean = true,
      numPartitions: OptionMagnet[Int] = None
  )(
      implicit
      ctag: ClassTag[D],
      lemma: Ordering[D]
  ) = {

    sortBy[D](v => v.data, ascending, numPartitions)
  }

//  /** TODO: remove, caching should be handled else
//    * only 1 RDD/Dataset will be cached at a time, if a cached instance with a higher precedence is computed, other
//    * instances with lower precedence will be dropped, its data representation will instead be reconstructed from
//    */
//  trait Cached {}

  def execute(): this.type = {
    this.rdd.foreach(_ => ())
    this
  }

  object flatMap {

    def apply[O: ClassTag](
        fn: ChainPlan.FlatMap._Fn[D, O]
    )(
        implicit
        sampling: Sampler = spooky.conf.selectSampling
    ): SpookyDataset[O] = {

      SpookyDataset(
        ChainPlan(
          SpookyDataset.this,
          ChainPlan.FlatMap.normalise(fn).andThen(v => sampling(v))
        )
      )
    }
  }
  def selectMany: flatMap.type = flatMap

  object map {

    def apply[O: ClassTag](
        fn: ChainPlan.Map._Fn[D, O]
    )(
        implicit
        sampling: Sampler = spooky.conf.selectSampling
    ): SpookyDataset[O] = {

      SpookyDataset(
        ChainPlan(
          SpookyDataset.this,
          ChainPlan.Map.normalise(fn).andThen(v => sampling(v))
        )
      )
    }
  }
  def select: map.type = map

  // Always left
  def fetch[ON, O: ClassTag](fn: FetchedRow[D] => ON)(
      implicit
      sampling: Sampler = spooky.conf.fetchSampling,
      keyBy: Trace => Any = identity,
      genPartitioner: GenPartitioner = spooky.conf.localityPartitioner,
      canFetch: CanFetch[ON, D, O]
      //      ctg: ClassTag[D]
  ): SpookyDataset[O] = {

    val normalForm: FetchPlan.Fn[D, O] = { inputRow => // TODO: trace/expose circuit
      val intermediate = fn(inputRow)

      val batch = canFetch.normalise(inputRow.data, intermediate)

      val sampled = sampling(batch)
      sampled
    }

    SpookyDataset(
      FetchPlan(plan, normalForm, keyBy, genPartitioner)(canFetch.cTag)
    )
  }

  case class exploreOn(
      range: Range = spooky.conf.exploreRange,
      pathPlanning: PathPlanning = spooky.conf.explorePathPlanning,
      //
      epochInterval: Int = spooky.conf.exploreEpochInterval,
      checkpointInterval: Int = spooky.conf.exploreCheckpointInterval // set to Int.MaxValue to disable checkpointing,
      //
      //      ordinalField: Field = null,
      //      depthField: Field = null, // TODO: Some of them has to be moved upwards
  ) {

    def fetch[
        ON // notice the lack of Output type as fetch here must be recursive
    ](fn: FetchedRow[Data.Exploring[D]] => ON)(
        implicit
        sampling: Sampler = spooky.conf.exploreSampling,
        keyBy: Trace => Any = identity,
        genPartitioner: GenPartitioner = spooky.conf.localityPartitioner,
        canFetch: CanFetch[ON, D, D]
    ): SpookyDataset[Data.Exploring[D]] = {

      val normalForm: FetchPlan.Fn[Data.Exploring[D], D] = { inputRow =>
        // TODO: remove duplication
        val intermediate = fn(inputRow)

        val batch = canFetch.normalise(inputRow.data, intermediate)

        val sampled = sampling(batch)
        sampled
      }

      val invarForm = ExplorePlan.Invar.normalise(normalForm)

      val params = Params(range)
      val out: ExplorePlan[D, Data.Exploring[D]] = ExplorePlan(
        plan,
        invarForm,
        keyBy,
        genPartitioner,
        params,
        pathPlanning,
        epochInterval,
        checkpointInterval
      )
      SpookyDataset(out)
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
