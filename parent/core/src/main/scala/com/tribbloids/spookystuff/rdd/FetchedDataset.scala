package com.tribbloids.spookystuff.rdd

import com.tribbloids.spookystuff.actions._
import com.tribbloids.spookystuff.conf.SpookyConf
import com.tribbloids.spookystuff.doc.{Doc, Fetched}
import com.tribbloids.spookystuff.dsl._
import com.tribbloids.spookystuff.execution.ExplorePlan.Params
import com.tribbloids.spookystuff.execution._
import com.tribbloids.spookystuff.extractors._
import com.tribbloids.spookystuff.extractors.impl.Get
import com.tribbloids.spookystuff.row.Field.Temp
import com.tribbloids.spookystuff.row._
import com.tribbloids.spookystuff.utils.SpookyViews
import com.tribbloids.spookystuff.{Const, SpookyContext}
import org.apache.spark.ml.dsl.utils.refl.CatalystTypeOps
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.catalyst.{CatalystTypeConverters, InternalRow}
import org.apache.spark.sql.types.{DataType, StructType}
import org.apache.spark.sql.utils.SparkHelper
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkContext, TaskContext}

import scala.collection.Map
import scala.collection.immutable.ListMap
import scala.concurrent.duration.Duration
import scala.language.implicitConversions
import scala.reflect.ClassTag

object FetchedDataset {

  implicit def FDToRDD(self: FetchedDataset): RDD[FetchedRow] = self.rdd
}

/**
  * Created by peng on 8/29/14. Core component, abstraction of distributed Page + schemaless KVStore to represent all
  * stages of remote resource discovery CAUTION: for bug tracking purpose it is important for all RDDs having their
  * names set to their {function name}.{variable names} CAUTION: naming convention: all function ended with _! will be
  * executed immediately, others will yield a logical plan that can be optimized & lazily executed
  */
case class FetchedDataset(
    plan: ExecutionPlan
) extends FetchedRDDAPI
    with CatalystTypeOps.ImplicitMixin {

  import SpookyViews._

  implicit def fromExecutionPlan(plan: ExecutionPlan): FetchedDataset = FetchedDataset(plan)

  def this(
      sourceRDD: SquashedFetchedRDD,
      fieldMap: ListMap[Field, DataType],
      spooky: SpookyContext,
      beaconRDDOpt: Option[BeaconRDD[TraceView]] = None
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
    f(spooky.spookyConf)
    this
  }

  def sparkContext: SparkContext = plan.spooky.sparkContext
  def storageLevel: StorageLevel = plan.storageLevel
  def storageLevel_=(lv: StorageLevel): Unit = {
    plan.storageLevel = lv
  }
  def isCached: Boolean = plan.isCached

  def squashedRDD: SquashedFetchedRDD = {
    plan.tryDeployAndRDD()
  }

  def rdd: RDD[FR] = unsquashedRDD
  def unsquashedRDD: RDD[FetchedRow] = this.squashedRDD.flatMap(v => v.WSchema(schema).unSquash)

  def docRDD: RDD[Seq[Fetched]] = {

    squashedRDD.map { row =>
      row.WSchema(schema).withSpooky.fetched
    }
  }

  def dataRDD: RDD[DataRow] = {

    squashedRDD.flatMap {
      _.dataRows
    }
  }

  def partitionRDD: RDD[(Int, Seq[FR])] = rdd.mapPartitions { ii =>
    Iterator(TaskContext.get().partitionId() -> ii.toSeq)
  }
  def partitionSizeRDD: RDD[(Int, Int)] = rdd.mapPartitions { ii =>
    Iterator(TaskContext.get().partitionId() -> ii.size)
  }

  def spooky: SpookyContext = plan.spooky
  def schema: SpookySchema = plan.schema
  def fields: List[Field] = schema.fields

  def dataRDDSorted: RDD[DataRow] = {

    import scala.Ordering.Implicits._ // DO NOT DELETE!

    val sortIndices: List[Field] = plan.allSortIndices.map(_._1.self)

    val dataRDD = this.dataRDD
    plan.persist(dataRDD)

    val sorted = dataRDD.sortBy(_.sortIndex(sortIndices))
    sorted.setName("sort")

    sorted.foreachPartition { _ => } // force execution TODO: remove, won't force
    plan.scratchRDDs.unpersist(dataRDD)

    sorted
  }
  def toMapRDD(sort: Boolean = false): RDD[Map[String, Any]] =
    sparkContext.withJob("toMapRDD", s"toMapRDD(sort=$sort)") {
      {
        if (!sort) dataRDD
        else dataRDDSorted
      }.map(_.toMap)
    }

  def toJSON(sort: Boolean = false): RDD[String] =
    sparkContext.withJob("toMapRDD", s"toJSON(sort=$sort)") {

      {
        if (!sort) dataRDD
        else dataRDDSorted
      }.map(_.compactJSON)
    }

  protected def toInternalRowRDD(
      sort: Boolean = false,
      spookySchema: SpookySchema
  ): RDD[InternalRow] = {

    val dataRDD =
      if (!sort) this.dataRDD
      else dataRDDSorted

    //    val field2Encoder: Map[Field, ExpressionEncoder[Any]] = spookySchema.fieldTypes.mapValues {
    //      tpe =>
    //        val ttg = tpe.asTypeTagCasted[Any]
    //        ExpressionEncoder.apply()(ttg)
    //    }

    // TOOD: how to make it serializable so it can be reused by different partitions?
    @transient lazy val field2Converter: Map[Field, Any => Any] = spookySchema.fieldTypes.map {
      case (k, tpe) =>
        val reified = tpe.reified
        val converter = CatalystTypeConverters.createToCatalystConverter(reified)
        k -> converter
    }

//    val rowEncoder = RowEncoder.apply(spookySchema.structType)

    dataRDD
      .map { v =>
        val converted: Seq[Any] = spookySchema.fields.map { field =>
          val raw: Any = v.data.get(field).orNull
          //              val encoder: ExpressionEncoder[Any] = field2Encoder(field)
          val converter = field2Converter(field)
          converter.apply(raw)
        }
        val internalRow = new GenericInternalRow(converted.toArray)

        internalRow
      }
  }

  def toDF(sort: Boolean = false): DataFrame =
    sparkContext.withJob("toDF", s"toDF(sort=$sort)") {

      val filteredSchema: SpookySchema = schema.filterFields()
      val sqlSchema: StructType = filteredSchema.structType
      val rowRDD = toInternalRowRDD(sort, filteredSchema)

      val result = SparkHelper.internalCreateDF(spooky.sqlContext, rowRDD, sqlSchema)

      result
    }

  def newResolver: SpookySchema#Resolver = schema.newResolver

  def toStringRDD(
      ex: Extractor[Any],
      default: String = null
  ): RDD[String] = {

    val _ex = newResolver.include(ex.toStr).head

    unsquashedRDD.map(v => _ex.applyOrElse[FetchedRow, String](v, _ => default))
  }

  def toObjectRDD[T: ClassTag](
      ex: Extractor[T],
      default: T = null
  ): RDD[T] = {

    val _ex = newResolver.include(ex).head

    unsquashedRDD.map(v => _ex.applyOrElse[FetchedRow, T](v, _ => default))
  }

  // IMPORTANT: DO NOT discard type parameter! otherwise arguments' type will be coerced into Any!
  def extract[T](exs: Extractor[T]*): FetchedDataset = {
    MapPlan.optimised(plan, MapPlan.Extract(exs))
  }

  def select[T](exprs: Extractor[T]*): FetchedDataset = extract(exprs: _*)

  def remove(fields: Field*): FetchedDataset = {
    MapPlan.optimised(plan, MapPlan.Remove(fields))
  }

  def removeWeaks(): FetchedDataset = this.remove(fields.filter(_.isWeak): _*)

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

    MapPlan.optimised(
      plan,
      MapPlan.SavePages(path.ex.typed[String], _extensionEx, _pageEx, overwrite)
    )
  }

  def explode(
      ex: Extractor[Any],
      forkType: ForkType = ForkType.defaultExplode,
      ordinalField: Field = null,
      sampler: Sampler[Any] = spooky.spookyConf.defaultExplodeSampler
  ): FetchedDataset = {

    val (on, extracted) = ex match {
      case Get(ff) =>
        ff -> this
      case _ =>
        val effectiveEx = ex.withTempAliasIfMissing
        val ff = effectiveEx.field
        ff -> this.extract(ex)
    }

    MapPlan.optimised(extracted.plan, MapPlan.Explode(on, ordinalField, sampler, forkType))
  }

  // TODO: test
  def agg(exprs: Seq[FetchedRow => Any], reducer: RowReducer): FetchedDataset = AggPlan(plan, exprs, reducer)
  def distinctBy(exprs: FetchedRow => Any*): FetchedDataset = agg(exprs, (v1, _) => v1)

  protected def _defaultCooldown(v: Option[Duration]): Trace = {
    val _delay: Trace = v.map { dd =>
      Delay(dd)
    }.toList
    _delay
  }

  protected def _defaultWget(
      cooldown: Option[Duration] = None,
      filter: DocFilter = Const.defaultDocumentFilter,
      on: Col[String] = Get(Temp)
  ): TraceView = {

    val _delay: Trace = _defaultCooldown(cooldown)

    val result = Wget(on, filter) +> _delay

    TraceView(result)
  }

  // Always left
  def fetch(
      traces: Set[Trace],
      keyBy: Trace => Any = identity,
      genPartitioner: GenPartitioner = spooky.spookyConf.defaultGenPartitioner
  ): FetchedDataset = {

    val _traces = TraceSetView(traces).rewriteGlobally(schema)

    FetchPlan(plan, _traces, keyBy, genPartitioner)
  }

  // shorthand of fetch
  def wget(
      on: Col[String],
      cooldown: Option[Duration] = None,
      keyBy: Trace => Any = identity,
      filter: DocFilter = Const.defaultDocumentFilter,
      failSafe: Int = -1,
      genPartitioner: GenPartitioner = spooky.spookyConf.defaultGenPartitioner
  ): FetchedDataset = {

    var trace = _defaultWget(cooldown, filter, on)

    if (failSafe > 0) trace = ClusterRetry(trace, failSafe)

    this.fetch(
      trace.asTraceSet,
      keyBy,
      genPartitioner = genPartitioner
    )
  }

  def fork(
      on: Extractor[Any], // name is discarded
      forkType: ForkType = ForkType.defaultFork,
      ordinalField: Field = null, // left & idempotent parameters are missing as they are always set to true
      sampler: Sampler[Any] = spooky.spookyConf.defaultForkSampler
  ): FetchedDataset = {

    val result = FetchedDataset.this
      .explode(on.withTempAliasIfMissing, forkType, ordinalField, sampler)
    result
  }

  /**
    * same as join, but avoid launching a browser by using direct http GET (wget) to download new pages much faster and
    * less stressful to both crawling and target server(s)
    *
    * @return
    *   RDD[Page]
    */
  def wgetFork(
      on: Extractor[Any],
      forkType: ForkType = ForkType.defaultFork,
      ordinalField: Field = null, // left & idempotent parameters are missing as they are always set to true
      sampler: Sampler[Any] = spooky.spookyConf.defaultForkSampler,
      cooldown: Option[Duration] = None,
      keyBy: Trace => Any = identity,
      filter: DocFilter = Const.defaultDocumentFilter,
      failSafe: Int = -1,
      genPartitioner: GenPartitioner = spooky.spookyConf.defaultGenPartitioner
  ): FetchedDataset = {

    var trace = _defaultWget(cooldown, filter)
    if (failSafe > 0) {
      trace = ClusterRetry(trace, failSafe).traceView
    }

    this
      .fork(on, forkType, ordinalField, sampler)
      .fetch(
        trace,
        keyBy,
        genPartitioner = genPartitioner
      )
  }

  // TODO: how to unify this with join?
  def explore(
      on: Extractor[Any],
      forkType: ForkType = ForkType.defaultExplore,
      ordinalField: Field = null,
      sampler: Sampler[Any] = spooky.spookyConf.defaultForkSampler
  )(
      traces: Set[Trace],
      keyBy: Trace => Any = identity,
      genPartitioner: GenPartitioner = spooky.spookyConf.defaultGenPartitioner,
      depthField: Field = null,
      range: Range = spooky.spookyConf.defaultExploreRange,
      exploreAlgorithm: ExploreAlgorithm = spooky.spookyConf.defaultExploreAlgorithm,
      epochSize: Int = spooky.spookyConf.epochSize,
      checkpointInterval: Int = spooky.spookyConf.checkpointInterval // set to Int.MaxValue to disable checkpointing,
  )(
      extracts: Extractor[_]*
      // apply immediately after depth selection, this include depth0
  ): FetchedDataset = {

    val params = Params(depthField, ordinalField, range)

    ExplorePlan(
      plan,
      on.withTempAliasIfMissing,
      sampler,
      forkType,
      TraceSetView(traces).rewriteGlobally(plan.schema),
      keyBy,
      genPartitioner,
      params,
      exploreAlgorithm,
      epochSize,
      checkpointInterval,
      List(MapPlan.Extract(extracts))
    )
  }

  def wgetExplore(
      on: Extractor[Any],
      forkType: ForkType = ForkType.defaultExplore,
      ordinalField: Field = null,
      sampler: Sampler[Any] = spooky.spookyConf.defaultForkSampler,
      filter: DocFilter = Const.defaultDocumentFilter,
      failSafe: Int = -1,
      cooldown: Option[Duration] = None,
      keyBy: Trace => Any = identity,
      genPartitioner: GenPartitioner = spooky.spookyConf.defaultGenPartitioner,
      depthField: Field = null,
      range: Range = spooky.spookyConf.defaultExploreRange,
      exploreAlgorithm: ExploreAlgorithm = spooky.spookyConf.defaultExploreAlgorithm,
      miniBatch: Int = 500,
      checkpointInterval: Int = spooky.spookyConf.checkpointInterval, // set to Int.MaxValue to disable checkpointing,

      select: Extractor[Any] = null,
      selects: Iterable[Extractor[Any]] = Seq()
  ): FetchedDataset = {

    var trace = _defaultWget(cooldown, filter)
    if (failSafe > 0) trace = ClusterRetry(trace, failSafe)

    explore(on, forkType, ordinalField, sampler)(
      trace,
      keyBy,
      genPartitioner,
      depthField,
      range,
      exploreAlgorithm,
      miniBatch,
      checkpointInterval
    )(
      Option(select).toSeq ++ selects: _*
    )
  }
}
