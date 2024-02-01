package com.tribbloids.spookystuff.rdd

import com.tribbloids.spookystuff.actions._
import com.tribbloids.spookystuff.conf.SpookyConf
import com.tribbloids.spookystuff.doc.Doc
import com.tribbloids.spookystuff.dsl._
import com.tribbloids.spookystuff.execution.Delta._
import com.tribbloids.spookystuff.execution.ExplorePlan.Params
import com.tribbloids.spookystuff.execution._
import com.tribbloids.spookystuff.extractors._
import com.tribbloids.spookystuff.extractors.impl.Get
import com.tribbloids.spookystuff.row._
import com.tribbloids.spookystuff.utils.SpookyViews
import com.tribbloids.spookystuff.{Const, SpookyContext}
import org.apache.spark.SparkContext
import org.apache.spark.ml.dsl.utils.refl.CatalystTypeOps
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.catalyst.{CatalystTypeConverters, InternalRow}
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.utils.SparkHelper
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.storage.StorageLevel

import scala.collection.Map
import scala.collection.immutable.ListMap
import scala.concurrent.duration.Duration
import scala.language.implicitConversions
import scala.reflect.ClassTag

object FetchedDataset extends FetchedDatasetImp0 {

  implicit def asRDD(self: FetchedDataset): RDD[FetchedRow] = self.rdd
}

/**
  * Created by peng on 8/29/14. Core component, abstraction of distributed Page + schemaless KVStore to represent all
  * stages of remote resource discovery CAUTION: for bug tracking purpose it is important for all RDDs having their
  * names set to their {function name}.{variable names} CAUTION: naming convention: all function ended with _! will be
  * executed immediately, others will yield a logical plan that can be optimized & lazily executed
  */
case class FetchedDataset(
    plan: ExecutionPlan
) extends FetchedDatasetAPI
    with CatalystTypeOps.ImplicitMixin {

  import SpookyViews._

  implicit def fromExecutionPlan(plan: ExecutionPlan): FetchedDataset = FetchedDataset(plan)

  def this(
      sourceRDD: SquashedRDD,
      fieldMap: ListMap[Field, DataType],
      spooky: SpookyContext,
      beaconRDDOpt: Option[BeaconRDD[LocalityGroup]] = None
  ) = {

    this(
      RDDPlan(
        SpookySchema(SpookyExecutionContext(spooky), fieldMap),
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
  def isCached: Boolean = plan.isCached

  def rdd: RDD[FR] = this.fetchedRDD

  def spooky: SpookyContext = plan.spooky
  def schema: SpookySchema = plan.outputSchema
  def fields: List[Field] = schema.fields

  def dataRDDSorted: RDD[DataRow] = {

    import scala.Ordering.Implicits._ // DO NOT DELETE!

    val sortIndices: List[Field] = plan.allSortIndices.map(_._1.self)

    val dataRDD = this.dataRDD
    plan.scratchRDDPersist(dataRDD)

    val sorted = dataRDD.sortBy(v => v.sortIndex(sortIndices: _*))
    sorted.setName("sort")

    sorted.foreachPartition { _ => } // force execution TODO: remove, won't force
    plan.scratchRDDs.unpersist(dataRDD)

    sorted
  }
  def toMapRDD(sort: Boolean = false): RDD[Map[String, Any]] =
    spooky.withJob("toMapRDD", s"toMapRDD(sort=$sort)") {
      {
        if (!sort) this.dataRDD
        else dataRDDSorted
      }.map(_.toMap)
    }

  def toJSON(sort: Boolean = false): Dataset[String] = {
    toDF(sort).toJSON
  }

  protected def toInternalRowRDD(
      sort: Boolean = false,
      spookySchema: SpookySchema
  ): RDD[InternalRow] = {

    val dataRDD =
      if (!sort) this.dataRDD
      else dataRDDSorted

    // TOOD: how to make it serializable so it can be reused by different partitions?
    @transient lazy val field2Converter: Map[Field, Any => Any] = spookySchema.fieldTypes.map {
      case (k, tpe) =>
        val reified = tpe.reified
        val converter = CatalystTypeConverters.createToCatalystConverter(reified)
        k -> converter
    }

    dataRDD
      .map { v =>
        val converted: Seq[Any] = spookySchema.fields.map { field =>
          val raw: Any = v.data.get(field).orNull
          //              val encoder: ExpressionEncoder[Any] = field2Encoder(field)
          val converter = field2Converter(field)
          converter.apply(raw)
        }
        val InternalRow = new GenericInternalRow(converted.toArray)

        InternalRow
      }
  }

  def toDF(
      sort: Boolean = false,
      removeTransient: Boolean = true
  ): DataFrame =
    spooky.withJob("toDF", s"toDF(sort=$sort)") {

      val effectiveSchema = {

        if (removeTransient) this.transientRemoved.schema
        else this.schema
      }

      val rowRDD = this.toInternalRowRDD(sort, effectiveSchema)

      val result = SparkHelper.internalCreateDF(spooky.sqlContext, rowRDD, effectiveSchema.structType)

      result
    }

  def newResolver: SpookySchema#Resolver = schema.newResolver

  def toStringRDD(
      ex: Extractor[Any],
      default: String = null
  ): RDD[String] = {

    val _ex = newResolver.include(ex.toStr).head

    this.fetchedRDD.map(v => _ex.applyOrElse[FetchedRow, String](v, _ => default))
  }

  def toObjectRDD[T: ClassTag](
      ex: Extractor[T],
      default: T = null
  ): RDD[T] = {

    val _ex = newResolver.include(ex).head

    this.fetchedRDD.map(v => _ex.applyOrElse[FetchedRow, T](v, _ => default))
  }

  // IMPORTANT: DO NOT discard type parameter! otherwise arguments' type will be coerced into Any!
  def extract[T](exs: Extractor[T]*): FetchedDataset = {
    DeltaPlan.optimised(plan, Extract(exs))
  }

  def apply[T](exs: Extractor[T]*): FetchedDataset = {
    DeltaPlan.optimised(plan, Extract(exs))
  }

  def remove(fields: Field*): FetchedDataset = {
    DeltaPlan.optimised(plan, Remove(fields))
  }

  def explodeObservations(
      fn: DataRow.WithScope => Seq[DataRow.WithScope]
  ): FetchedDataset = {
    DeltaPlan.optimised(plan, ExplodeScope(fn))
  }

  def transientRemoved: FetchedDataset = this.remove(fields.filter(_.isTransient): _*)

  /**
    * this is an action that will be triggered immediately
    */
  def savePages_!(
      path: Col[String],
      extension: Col[String] = null,
      page: Extractor[Doc] = S,
      overwrite: Boolean = false
  ): this.type = {
    val saved = savePages(path, extension, page, overwrite)
    saved.rdd.forceExecute()
    this
  }

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

    DeltaPlan.optimised(
      plan,
      SavePages(path.ex.typed[String], _extensionEx, _pageEx, overwrite)
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

    DeltaPlan.optimised(extracted.plan, ExplodeData(on, ordinalField, sampler, forkType))
  }

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

  protected def getCooldown(v: Option[Duration]): Trace = {
    val _delay: Trace = v.map { dd =>
      Delay(dd)
    }.toList
    _delay
  }

  protected def _defaultWget(
      cooldown: Option[Duration] = None,
      filter: DocFilter = Const.defaultDocumentFilter,
      on: Col[String] = Get(Const.defaultForkField)
  ): Trace = {

    val _delay: Trace = getCooldown(cooldown)

    val result = Wget(on, filter) +> _delay

    Trace(result)
  }

  // Always left
  def fetch(
      traces: HasTraceSet,
      keyBy: Trace => Any = identity,
      genPartitioner: GenPartitioner = spooky.conf.localityPartitioner
  ): FetchedDataset = {

    val _traces = traces.asTraceSet.rewriteGlobally(schema)

    FetchPlan(plan, _traces, keyBy, genPartitioner)
  }

  // shorthand of fetch
  def wget(
      on: Col[String],
      cooldown: Option[Duration] = None,
      keyBy: Trace => Any = identity,
      filter: DocFilter = Const.defaultDocumentFilter,
      failSafe: Int = -1,
      genPartitioner: GenPartitioner = spooky.conf.localityPartitioner
  ): FetchedDataset = {

    var trace: Trace = _defaultWget(cooldown, filter, on)

    if (failSafe > 0) trace = Trace.of(ClusterRetry(trace, failSafe))

    this.fetch(
      trace.asTraceSet,
      keyBy,
      genPartitioner = genPartitioner
    )
  }

  def wgetFork(
      on: Extractor[Any],
      forkType: ForkType = ForkType.default,
      ordinalField: Field = null, // left & idempotent parameters are missing as they are always set to true
      sampler: Sampler[Any] = spooky.conf.forkSampler,
      cooldown: Option[Duration] = None,
      keyBy: Trace => Any = identity,
      filter: DocFilter = Const.defaultDocumentFilter,
      failSafe: Int = -1,
      genPartitioner: GenPartitioner = spooky.conf.localityPartitioner
  ): FetchedDataset = {

    var trace: Trace = _defaultWget(cooldown, filter)
    if (failSafe > 0) {
      trace = ClusterRetry(trace, failSafe).trace
    }

    this
      .fork(on, forkType, ordinalField, sampler)
      .fetch(
        trace.asTraceSet,
        keyBy,
        genPartitioner = genPartitioner
      )
  }

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
      depthField: Field = null,
      range: Range = spooky.conf.exploreRange,
      exploreAlgorithm: PathPlanning = spooky.conf.explorePathPlanner,
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
      exploreAlgorithm,
      epochSize,
      checkpointInterval,
      Nil
    )
  }

  def wgetExplore(
      on: Extractor[Any],
      forkType: ForkType = ForkType.default,
      ordinalField: Field = null,
      sampler: Sampler[Any] = spooky.conf.forkSampler,
      filter: DocFilter = Const.defaultDocumentFilter,
      failSafe: Int = -1,
      cooldown: Option[Duration] = None,
      keyBy: Trace => Any = identity,
      genPartitioner: GenPartitioner = spooky.conf.localityPartitioner,
      depthField: Field = null,
      range: Range = spooky.conf.exploreRange,
      exploreAlgorithm: PathPlanning = spooky.conf.explorePathPlanner,
      miniBatch: Int = 500,
      checkpointInterval: Int = spooky.conf.exploreCheckpointInterval // set to Int.MaxValue to disable checkpointing,

  ): FetchedDataset = {

    var trace = _defaultWget(cooldown, filter)
    if (failSafe > 0) trace = Trace.of(ClusterRetry(trace, failSafe))

    explore(on, forkType, ordinalField, sampler)(
      trace.asTraceSet,
      keyBy,
      genPartitioner,
      depthField,
      range,
      exploreAlgorithm,
      miniBatch,
      checkpointInterval
    )
  }
}
