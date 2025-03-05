package com.tribbloids.spookystuff.rdd

import ai.acyclic.prover.commons.util.Magnet.OptionMagnet
import com.tribbloids.spookystuff.SpookyContext
import com.tribbloids.spookystuff.actions.*
import com.tribbloids.spookystuff.commons.refl.CatalystTypeOps
import com.tribbloids.spookystuff.conf.SpookyConf
import com.tribbloids.spookystuff.dsl.*
import com.tribbloids.spookystuff.execution.*
import com.tribbloids.spookystuff.execution.ExplorePlan.Params
import com.tribbloids.spookystuff.execution.FetchPlan.Batch
import com.tribbloids.spookystuff.row.*
import frameless.{TypedDataset, TypedEncoder}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.storage.StorageLevel

import scala.language.implicitConversions
import scala.reflect.ClassTag

object DataView extends SpookyDataset_Imp0 {

  implicit def asRDD[D](self: DataView[D]): RDD[FetchedRow[D]] = self.rdd

  implicit class TypedDatasetView[D](
      ops: DataView[D]
  )(
      implicit
      ctag: ClassTag[D],
      enc: TypedEncoder[D]
  ) {

    final def toFrameless: TypedDataset[D] = {

      val ds = TypedDataset.create(ops.dataRDD)(enc, ops.ctx.sparkSession)

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
      ctx: SpookyContext,
      beaconRDDOpt: Option[BeaconRDD[LocalityGroup]] = None
  ): DataView[D] = {

    DataView(
      RDDPlan(
        SpookySchema(ExecutionContext(ctx)),
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
case class DataView[D](
    private val _plan: ExecutionPlan[D]
) extends DataViewAPI[D]
    with CatalystTypeOps.ImplicitMixin {
  // TODO: should be "ExecutionPlanView"

  // TODO: use reflection for more clear API
  def setConf(f: SpookyConf => Unit): this.type = {
    f(ctx.conf)
    this
  }

  val plan: ExecutionPlan[D] = _plan.normalise

  def sparkContext: SparkContext = plan.ctx.sparkContext
  def storageLevel: StorageLevel = plan.storageLevel
  def storageLevel_=(lv: StorageLevel): Unit = {
    plan.storageLevel = lv
  }

  def ctx: SpookyContext = plan.ctx
  def schema: SpookySchema = plan.outputSchema

  def isCached: Boolean = plan.isCached

  def dataRDD(
      implicit
      ctag: ClassTag[D]
  ): RDD[D] = {
    this.rdd.map(_.data)
  }

  def sortBy[E: ClassTag: Ordering](
      fn: SortPlan.Fn[D, E],
      ascending: Boolean = true,
      numPartitions: OptionMagnet[Int] = None
  ): DataView[D] = {
    val plan = SortPlan(this.plan, fn, ascending, numPartitions)

    DataView(plan)
  }

  def sorted(
      ascending: Boolean = true,
      numPartitions: OptionMagnet[Int] = None
  )(
      implicit
      ctag: ClassTag[D],
      lemma: Ordering[D]
  ): DataView[D] = {

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
        fn: FlatMapPlan.FlatMap._Fn[D, O]
    )(
        implicit
        sampling: Sampler = ctx.conf.selectSampling
    ): DataView[O] = {

      DataView(
        FlatMapPlan(
          DataView.this,
          FlatMapPlan.FlatMap.normalise(fn).andThen(v => sampling(v))
        )
      )
    }
  }
  def selectMany: flatMap.type = flatMap

  object map {

    def apply[O: ClassTag](
        fn: FlatMapPlan.Map._Fn[D, O]
    )(
        implicit
        sampling: Sampler = ctx.conf.selectSampling
    ): DataView[O] = {

      DataView(
        FlatMapPlan(
          DataView.this,
          FlatMapPlan.Map.normalise(fn).andThen(v => sampling(v))
        )
      )
    }
  }
  def select: map.type = map

  def foreach(fn: FlatMapPlan.Map._Fn[D, Unit] = { _ => () }) = {

    map { row =>
      row.localityGroup.withCtx(ctx).trajectory
      // always execute the agent eagerly
      fn(row)
    }
  }

  // Always left
  def fetch[
      FO, // function output
      O: ClassTag
  ](
      fn: FetchedRow[D] => FO,
      sampling: Sampler = ctx.conf.fetchSampling,
      keyBy: Trace => Any = identity,
      genPartitioner: Locality = ctx.conf.genPartitioner
  )(
      implicit
      canFetch: CanFetch[FO, D, O]
  ): DataView[O] = {

    val normalForm: FetchPlan.Fn[D, O] = { inputRow => // TODO: trace/expose circuit
      val intermediate = fn(inputRow)

      val batch = canFetch.normaliseOutput(inputRow.data, intermediate)

      val sampled = sampling(batch)
      sampled
    }

    DataView(
      FetchPlan(plan, normalForm, keyBy, genPartitioner)(canFetch.cTag)
    )
  }

  def recursively(
      range: Range = ctx.conf.exploreRange,
      pathPlanning: PathPlanning = ctx.conf.explorePathPlanning,
      //
      epochInterval: Int = ctx.conf.exploreEpochInterval,
      checkpointInterval: Int = ctx.conf.exploreCheckpointInterval // set to Int.MaxValue to disable checkpointing,
  ): RecursiveView[D] = RecursiveView(
    fnBeforeRecursion = { v =>
      Seq(v.data.raw)
    },
    range = range,
    pathPlanning = pathPlanning,
    epochInterval = epochInterval,
    checkpointInterval = checkpointInterval
  )

  case class RecursiveView[
      M // intermediate value before recursion
  ](
      fnBeforeRecursion: FlatMapPlan.Fn[
        Data.Exploring[D],
        M
      ],
      range: Range = ctx.conf.exploreRange,
      pathPlanning: PathPlanning = ctx.conf.explorePathPlanning,
      //
      epochInterval: Int = ctx.conf.exploreEpochInterval,
      checkpointInterval: Int = ctx.conf.exploreCheckpointInterval // set to Int.MaxValue to disable checkpointing,
  ) {

    val transformRowBeforeRecursion: FetchedRow[Data.Exploring[D]] => Seq[FetchedRow[Data.Exploring[M]]] = { row =>
      val data = fnBeforeRecursion(row).map { _raw =>
        row.data.copy(raw = _raw)
      }

      data.map { newData =>
        row.copy(
          data = newData
        )
      }
    }

    def explore[
        FO // function output, notice the lack of Output type as fetch here must be recursive
    ](
        fn: FetchedRow[Data.Exploring[M]] => FO,
        sampling: Sampler = ctx.conf.exploreSampling,
        keyBy: Trace => Any = identity,
        genPartitioner: Locality = ctx.conf.genPartitioner
    )(
        implicit
        canFetch: CanFetch[FO, D, D]
    ): DataView[M] = {

      val normalForm: ExplorePlan.Fn[D, M] = { inputRow =>
        // TODO: remove duplication

        val intermediate: Seq[FetchedRow[Data.Exploring[M]]] = transformRowBeforeRecursion(inputRow)

        val batch = intermediate.flatMap { row =>
          val fo: FO = fn(row)

          val batch: Batch[D] = canFetch.normaliseOutput(inputRow.data.raw, fo)
          batch
        }
        val recursive = sampling(batch)
        val out = intermediate.map(_.data.raw)

        recursive -> out
      }

      val params = Params(range)
      val out: ExplorePlan[D, M] = ExplorePlan(
        plan,
        normalForm,
        keyBy,
        genPartitioner,
        params,
        pathPlanning,
        epochInterval,
        checkpointInterval
      )
      DataView(out)
    }

    //    object flatMap {}
    // TODO: all selected data prior to fetch will become inductive data update
    //    object map {}

    object flatMap {

      def apply[MM](
          fn: FlatMapPlan.FlatMap._Fn[Data.Exploring[M], MM]
      )(
          implicit
          sampling: Sampler = ctx.conf.selectSampling
      ): RecursiveView[MM] = {

        val fnNormalised: FlatMapPlan.Fn[Data.Exploring[M], MM] = FlatMapPlan.FlatMap.normalise(fn)

        val chained: FlatMapPlan.Fn[Data.Exploring[D], MM] = { row =>
          val before: Seq[FetchedRow[Data.Exploring[M]]] = transformRowBeforeRecursion(row)

          val after = before.flatMap { row =>
            fnNormalised(row)
          }

          after
        }

        RecursiveView.this.copy[MM](
          fnBeforeRecursion = chained
        )
      }
    }
    def selectMany: flatMap.type = flatMap

    object map {

      def apply[MM](
          fn: FlatMapPlan.Map._Fn[Data.Exploring[M], MM]
      )(
          implicit
          sampling: Sampler = ctx.conf.selectSampling
      ): RecursiveView[MM] = {

        flatMap(FlatMapPlan.Map.normalise(fn))
      }
    }
    def select: map.type = map
  }
}
