package com.tribbloids.spookystuff.rdd

import com.tribbloids.spookystuff.actions._
import com.tribbloids.spookystuff.conf.SpookyConf
import com.tribbloids.spookystuff.doc.{Doc, DocOption}
import com.tribbloids.spookystuff.dsl._
import com.tribbloids.spookystuff.execution.ExplorePlan.Params
import com.tribbloids.spookystuff.execution._
import com.tribbloids.spookystuff.extractors._
import com.tribbloids.spookystuff.extractors.impl.Get
import com.tribbloids.spookystuff.row._
import com.tribbloids.spookystuff.utils.SpookyViews
import com.tribbloids.spookystuff.{Const, SpookyContext}
import org.apache.spark.ml.dsl.utils.refl.CatalystTypeOps
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.catalyst.{CatalystTypeConverters, InternalRow}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.utils.SparkHelper
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkContext, TaskContext}

import scala.collection.Map
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
      fields: List[Field],
      spooky: SpookyContext,
      beaconRDDOpt: Option[BeaconRDD[TraceView]] = None
  ) = {

    this(
      RDDPlan(
        SpookySchema(SpookyExecutionContext(spooky), fields),
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
  def unsquashedRDD: RDD[FetchedRow] = this.squashedRDD.flatMap(v => v.WSchema(schema).unsquash)

  def docRDD: RDD[Seq[DocOption]] = {

    squashedRDD.map { row =>
      row.WSchema(schema).withSpooky.getDoc
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

  def dataRDDSorted: RDD[DataRow] = {

    import scala.Ordering.Implicits._ // DO NOT DELETE!

    val sortAliases: List[Alias] = plan.allSortIndices.map(_.alias)

    val dataRDD = this.dataRDD
    plan.persist(dataRDD)

    val sorted = dataRDD.sortBy(_.sortIndex(sortAliases))
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

    // TODO: how to make it serializable so it can be reused by different partitions?
    @transient lazy val index2Converter: Map[Field.Symbol, Any => Any] = spookySchema.fieldLookup.map {
      case (k, tpe) =>
        val reified = tpe.dataType.reified
        val converter = CatalystTypeConverters.createToCatalystConverter(reified)
        k -> converter
    }

    dataRDD
      .map { v =>
        val converted: Seq[Any] = spookySchema.indices.map { field =>
          val raw: Any = v.data.get(field).orNull
          //              val encoder: ExpressionEncoder[Any] = field2Encoder(field)
          val converter = index2Converter(field)
          converter.apply(raw)
        }
        val internalRow = new GenericInternalRow(converted.toArray)

        internalRow
      }
  }

  def toDF(sort: Boolean = false): DataFrame =
    sparkContext.withJob("toDF", s"toDF(sort=$sort)") {

      val filteredSchema: SpookySchema = schema.filterBy()
      val sqlSchema: StructType = filteredSchema.inSpark.structType
      val rowRDD = toInternalRowRDD(sort, filteredSchema)

      val result = SparkHelper.internalCreateDF(spooky.sqlContext, rowRDD, sqlSchema)

      result
    }

  def newResolver: SpookySchema#ResolveDelta = schema.ResolveDelta

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

  def remove(indices: Field.Symbol*): FetchedDataset = {
    MapPlan.optimised(plan, MapPlan.Remove(indices))
  }

  def removeWeak(): FetchedDataset = this.remove(schema.indices.filter(_.isWeak): _*)

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

    val _pageEx: Extractor[Doc] = page.ex.filterByType[Doc]

    val _extensionEx: Extractor[String] = Option(extension)
      .map(_.ex.filterByType[String])
      .getOrElse(_pageEx.defaultFileExtension)

    MapPlan.optimised(
      plan,
      MapPlan.SavePages(path.ex.filterByType[String], _extensionEx, _pageEx, overwrite)
    )
  }

  def flatten(
      ex: Extractor[Any],
      isLeft: Boolean = true,
      ordinal: Alias = null,
      sampler: Sampler[Any] = spooky.spookyConf.defaultFlattenSampler
  ): FetchedDataset = {

    val (on, extracted) = ex match {
      case Get(ff) =>
        ff -> this
      case _ =>
        val effectiveEx = ex.withJoinFieldIfMissing
        val ff = effectiveEx.alias
        ff -> this.extract(ex)
    }

    MapPlan.optimised(extracted.plan, MapPlan.Flatten(on, ordinal, sampler, isLeft))
  }

  /**
    * break each page into 'shards', used to extract structured data from tables
    * @param on
    *   denotes enclosing elements of each shards
    */
  def flatExtract(
      on: Extractor[Any], // TODO: used to be Iterable[Unstructured], any tradeoff?
      isLeft: Boolean = true,
      ordinal: Alias = null,
      sampler: Sampler[Any] = spooky.spookyConf.defaultFlattenSampler
  )(exprs: Extractor[Any]*): FetchedDataset = {
    this
      .flatten(on.withJoinFieldIfMissing, isLeft, ordinal, sampler)
      .extract(exprs: _*)
  }

  def flatSelect(
      on: Extractor[Any], // TODO: used to be Iterable[Unstructured], any tradeoff?
      ordinal: Alias = null,
      sampler: Sampler[Any] = spooky.spookyConf.defaultFlattenSampler,
      isLeft: Boolean = true
  )(exprs: Extractor[Any]*): FetchedDataset = flatExtract(on, isLeft, ordinal, sampler)(exprs: _*)

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
      on: Col[String] = Get(Const.defaultJoin)
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

  def join(
      on: Extractor[Any], // name is discarded
      joinType: JoinType = spooky.spookyConf.defaultJoinType,
      ordinal: Alias = null, // left & idempotent parameters are missing as they are always set to true
      sampler: Sampler[Any] = spooky.spookyConf.defaultJoinSampler
  )(
      traces: Set[Trace],
      keyBy: Trace => Any = identity,
      genPartitioner: GenPartitioner = spooky.spookyConf.defaultGenPartitioner
  ): FetchedDataset = {

    val flat = this
      .flatten(on.withJoinFieldIfMissing, joinType.isLeft, ordinal, sampler)

    flat.fetch(traces, keyBy, genPartitioner)
  }

  /**
    * same as join, but avoid launching a browser by using direct http GET (wget) to download new pages much faster and
    * less stressful to both crawling and target server(s)
    *
    * @return
    *   RDD[Page]
    */
  def wgetJoin(
      on: Extractor[Any],
      joinType: JoinType = spooky.spookyConf.defaultJoinType,
      ordinal: Alias = null, // left & idempotent parameters are missing as they are always set to true
      sampler: Sampler[Any] = spooky.spookyConf.defaultJoinSampler,
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

    this.join(on, joinType, ordinal, sampler)(
      trace,
      keyBy,
      genPartitioner = genPartitioner
    )
  }

  // TODO: how to unify this with join?
  def explore(
      on: Extractor[Any],
      joinType: JoinType = spooky.spookyConf.defaultJoinType,
      ordinal: Alias = null,
      sampler: Sampler[Any] = spooky.spookyConf.defaultJoinSampler
  )(
      traces: Set[Trace],
      keyBy: Trace => Any = identity,
      genPartitioner: GenPartitioner = spooky.spookyConf.defaultGenPartitioner,
      depth: Alias = null,
      range: Range = spooky.spookyConf.defaultExploreRange,
      exploreAlgorithm: ExploreAlgorithm = spooky.spookyConf.defaultExploreAlgorithm,
      epochSize: Int = spooky.spookyConf.epochSize,
      checkpointInterval: Int = spooky.spookyConf.checkpointInterval // set to Int.MaxValue to disable checkpointing,
  )(
      extracts: Extractor[_]*
      // apply immediately after depth selection, this include depth0
  ): FetchedDataset = {

    val params = Params(depth, ordinal, range)

    ExplorePlan(
      plan,
      on.withJoinFieldIfMissing,
      sampler,
      joinType,
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
      joinType: JoinType = spooky.spookyConf.defaultJoinType,
      ordinal: Alias = null,
      sampler: Sampler[Any] = spooky.spookyConf.defaultJoinSampler,
      filter: DocFilter = Const.defaultDocumentFilter,
      failSafe: Int = -1,
      cooldown: Option[Duration] = None,
      keyBy: Trace => Any = identity,
      genPartitioner: GenPartitioner = spooky.spookyConf.defaultGenPartitioner,
      depth: Alias = null,
      range: Range = spooky.spookyConf.defaultExploreRange,
      exploreAlgorithm: ExploreAlgorithm = spooky.spookyConf.defaultExploreAlgorithm,
      miniBatch: Int = 500,
      checkpointInterval: Int = spooky.spookyConf.checkpointInterval, // set to Int.MaxValue to disable checkpointing,

      select: Extractor[Any] = null,
      selects: Iterable[Extractor[Any]] = Seq()
  ): FetchedDataset = {

    var trace = _defaultWget(cooldown, filter)
    if (failSafe > 0) trace = ClusterRetry(trace, failSafe)

    explore(on, joinType, ordinal, sampler)(
      trace,
      keyBy,
      genPartitioner,
      depth,
      range,
      exploreAlgorithm,
      miniBatch,
      checkpointInterval
    )(
      Option(select).toSeq ++ selects: _*
    )
  }
}
