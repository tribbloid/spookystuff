package com.tribbloids.spookystuff.rdd

import com.tribbloids.spookystuff.actions.{ClusterRetry, Snapshot, Visit, Wget, _}
import com.tribbloids.spookystuff.conf.SpookyConf
import com.tribbloids.spookystuff.doc.{Doc, DocOption}
import com.tribbloids.spookystuff.dsl.{ExploreAlgorithm, JoinType, _}
import com.tribbloids.spookystuff.execution.ExplorePlan.Params
import com.tribbloids.spookystuff.execution.{ExplorePlan, FetchPlan, _}
import com.tribbloids.spookystuff.extractors._
import com.tribbloids.spookystuff.extractors.impl.Get
import com.tribbloids.spookystuff.row.{Field, _}
import com.tribbloids.spookystuff.utils.SpookyViews
import com.tribbloids.spookystuff.{Const, SpookyContext}
import org.apache.spark.TaskContext
import org.apache.spark.ml.dsl.utils.refl.ScalaType
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.catalyst.{CatalystTypeConverters, InternalRow}
import org.apache.spark.sql.types.{DataType, StructType}
import org.apache.spark.sql.utils.SparkHelper
import org.apache.spark.storage.StorageLevel

import scala.collection.Map
import scala.collection.immutable.ListMap
import scala.concurrent.duration.Duration
import scala.language.implicitConversions
import scala.reflect.ClassTag

/**
  * Created by peng on 8/29/14.
  * Core component, abstraction of distributed Page + schemaless KVStore to represent all stages of remote resource discovery
  * CAUTION: for bug tracking purpose it is important for all RDDs having their names set to their {function name}.{variable names}
  * CAUTION: naming convention:
  * all function ended with _! will be executed immediately, others will yield a logical plan that can be optimized & lazily executed
  */
case class FetchedDataset(
    plan: ExecutionPlan
) extends FetchedRDDAPI {

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
        sourceRDD,
        SpookySchema(SpookyExecutionContext(spooky), fieldMap),
        beaconRDDOpt
      )
    )
  }

  //TODO: use reflection for more clear API
  def setConf(f: SpookyConf => Unit): this.type = {
    f(spooky.spookyConf)
    this
  }

  def sparkContext = plan.spooky.sparkContext
  def storageLevel: StorageLevel = plan.storageLevel
  def storageLevel_=(lv: StorageLevel) = {
    plan.storageLevel = lv
  }
  def isCached = plan.isCached

  def squashedRDD: SquashedFetchedRDD = {
    plan.broadcastAndRDD()
  }

  def rdd = unsquashedRDD
  def unsquashedRDD: RDD[FetchedRow] = this.squashedRDD.flatMap(
    v => v.WSchema(schema).unsquash
  )

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

  def partitionRDD = rdd.mapPartitions { ii =>
    Iterator(TaskContext.get().partitionId() -> ii.toSeq)
  }
  def partitionSizeRDD = rdd.mapPartitions { ii =>
    Iterator(TaskContext.get().partitionId() -> ii.size)
  }

  def spooky = plan.spooky
  def schema = plan.schema
  def fields = schema.fields

  def dataRDDSorted: RDD[DataRow] = {

    import scala.Ordering.Implicits._ //DO NOT DELETE!

    val sortIndices: List[Field] = plan.allSortIndices.map(_._1.self)

    val dataRDD = this.dataRDD
    plan.persist(dataRDD)

    val sorted = dataRDD.sortBy { _.sortIndex(sortIndices) }
    sorted.setName("sort")

    sorted.foreachPartition { _ =>
      } //force execution
    plan.scratchRDDs.unpersist(dataRDD)

    sorted
  }
  def toMapRDD(sort: Boolean = false): RDD[Map[String, Any]] = sparkContext.withJob(s"toMapRDD(sort=$sort)") {
    {
      if (!sort) dataRDD
      else dataRDDSorted
    }.map(_.toMap)
  }

  def toJSON(sort: Boolean = false): RDD[String] = sparkContext.withJob(s"toJSON(sort=$sort)") {

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

    import ScalaType._

    //    val field2Encoder: Map[Field, ExpressionEncoder[Any]] = spookySchema.fieldTypes.mapValues {
    //      tpe =>
    //        val ttg = tpe.asTypeTagCasted[Any]
    //        ExpressionEncoder.apply()(ttg)
    //    }

    //TOOD: how to make it serializable so it can be reused by different partitions?
    @transient lazy val field2Converter: Map[Field, Any => Any] = spookySchema.fieldTypes.mapValues { tpe =>
      val reified = tpe.reified
      val converter = CatalystTypeConverters.createToCatalystConverter(reified)
      converter
    }

    val rowEncoder = RowEncoder.apply(spookySchema.structType)

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
    sparkContext.withJob(s"toDF(sort=$sort)") {

      val filteredSchema: SpookySchema = schema.filterFields()
      val sqlSchema: StructType = filteredSchema.structType
      val rowRDD = toInternalRowRDD(sort, filteredSchema)

      val result = SparkHelper.internalCreateDF(spooky.sqlContext, rowRDD, sqlSchema)

      result
    }

  //TODO: cleanup
  //  @Deprecated
  //  def toDFLegacy(sort: Boolean = false, tableName: String = null): DataFrame =
  //    sparkContext.withJob(s"toDF(sort=$sort, name=$tableName)") {
  //
  //      val jsonRDD = this.toJSON(sort)
  //      plan.persist(jsonRDD)
  //
  //      val schemaRDD = spooky.sqlContext.read.json(jsonRDD)
  //
  //      val columns: Seq[Column] = fields
  //        .filter(key => !key.isWeak)
  //        .map {
  //          key =>
  //            val name = SpookyUtils.canonizeColumnName(key.name)
  //            if (schemaRDD.schema.fieldNames.contains(name)) new Column(UnresolvedAttribute(name))
  //            else new Column(expressions.Alias(org.apache.spark.sql.catalyst.expressions.Literal(null), name)())
  //        }
  //
  //      val result = schemaRDD.select(columns: _*)
  //
  //      if (tableName!=null) result.createOrReplaceTempView(tableName)
  //      plan.scratchRDDs.clearAll()
  //
  //      result
  //    }

  def newResolver = schema.newResolver

  def toStringRDD(
      ex: Extractor[Any],
      default: String = null
  ): RDD[String] = {

    val _ex = newResolver.include(ex.toStr).head

    unsquashedRDD.map(
      v => _ex.applyOrElse[FetchedRow, String](v, _ => default)
    )
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
    OptimizedMapPlan(plan, MapPlan.Extract(exs))
  }

  def select[T](exprs: Extractor[T]*) = extract(exprs: _*)

  def remove(fields: Field*): FetchedDataset = {
    OptimizedMapPlan(plan, MapPlan.Remove(fields))
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
    saved.foreach { _ =>
      }
    this
  }

  /**
    * save each page to a designated directory
    * support many file systems including but not limited to HDFS, S3 and local HDD
    *
    * @param overwrite if a file with the same name already exist:
    *                  true: overwrite it
    *                  false: append an unique suffix to the new file name
    */
  //always use the same path pattern for filtered pages, if you want pages to be saved with different path, use multiple saveContent with different names
  def savePages(
      path: Col[String],
      extension: Col[String] = null, //set to
      page: Col[Doc] = S,
      overwrite: Boolean = false
  ): FetchedDataset = {

    val _pageEx: Extractor[Doc] = page.ex.typed[Doc]

    val _extensionEx: Extractor[String] = Option(extension)
      .map(_.ex.typed[String])
      .getOrElse(_pageEx.defaultFileExtension)

    OptimizedMapPlan(plan, MapPlan.SavePages(path.ex.typed[String], _extensionEx, _pageEx, overwrite))
  }

  def flatten(
      ex: Extractor[Any],
      isLeft: Boolean = true,
      ordinalField: Field = null,
      sampler: Sampler[Any] = spooky.spookyConf.defaultFlattenSampler
  ): FetchedDataset = {

    val (on, extracted) = ex match {
      case Get(ff) =>
        ff -> this
      case _ =>
        val effectiveEx = ex.withJoinFieldIfMissing
        val ff = effectiveEx.field
        ff -> this.extract(ex)
    }

    OptimizedMapPlan(extracted.plan, MapPlan.Flatten(on, ordinalField, sampler, isLeft))
  }

  /**
    * break each page into 'shards', used to extract structured data from tables
    * @param on denotes enclosing elements of each shards
    */
  def flatExtract(
      on: Extractor[Any], //TODO: used to be Iterable[Unstructured], any tradeoff?
      isLeft: Boolean = true,
      ordinalField: Field = null,
      sampler: Sampler[Any] = spooky.spookyConf.defaultFlattenSampler
  )(exprs: Extractor[Any]*): FetchedDataset = {
    this
      .flatten(on.withJoinFieldIfMissing, isLeft, ordinalField, sampler)
      .extract(exprs: _*)
  }

  def flatSelect(
      on: Extractor[Any], //TODO: used to be Iterable[Unstructured], any tradeoff?
      ordinalField: Field = null,
      sampler: Sampler[Any] = spooky.spookyConf.defaultFlattenSampler,
      isLeft: Boolean = true
  )(exprs: Extractor[Any]*) = flatExtract(on, isLeft, ordinalField, sampler)(exprs: _*)

  //TODO: test
  def agg(exprs: Seq[FetchedRow => Any], reducer: RowReducer): FetchedDataset = AggPlan(plan, exprs, reducer)
  def distinctBy(exprs: FetchedRow => Any*): FetchedDataset = agg(exprs, (v1, v2) => v1)

  protected def _defaultCooldown(v: Option[Duration]) = {
    val _delay: Trace = v.map { dd =>
      Delay(dd)
    }.toList
    _delay
  }

  protected def _defaultVisit(
      cooldown: Option[Duration] = None,
      filter: DocFilter = Const.defaultDocumentFilter,
      on: Col[String] = Get(Const.defaultJoinField)
  ) = {

    val _cooldown: Duration = cooldown.getOrElse(Const.Interaction.delayMin)
    val result = (
      Visit(on, cooldown = _cooldown)
        +> Snapshot(filter)
    )

    result
  }

  protected def _defaultWget(
      cooldown: Option[Duration] = None,
      filter: DocFilter = Const.defaultDocumentFilter,
      on: Col[String] = Get(Const.defaultJoinField)
  ) = {

    val _delay: Trace = _defaultCooldown(cooldown)

    val result = Wget(on, filter) +> Set(_delay)

    result
  }

  // Always left
  def fetch(
      traces: Set[Trace],
      keyBy: Trace => Any = identity,
      genPartitioner: GenPartitioner = spooky.spookyConf.defaultGenPartitioner
  ): FetchedDataset = FetchPlan(plan, traces.rewriteGlobally(plan.schema), keyBy, genPartitioner)

  //shorthand of fetch
  def visit(
      on: Col[String],
      cooldown: Option[Duration] = None,
      keyBy: Trace => Any = identity,
      filter: DocFilter = Const.defaultDocumentFilter,
      failSafe: Int = -1,
      genPartitioner: GenPartitioner = spooky.spookyConf.defaultGenPartitioner
  ): FetchedDataset = {

    var trace = _defaultVisit(cooldown, filter, on)
    if (failSafe > 0) trace = ClusterRetry(trace, failSafe)

    this.fetch(
      trace,
      keyBy,
      genPartitioner = genPartitioner
    )
  }

  //shorthand of fetch
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
      trace,
      keyBy,
      genPartitioner = genPartitioner
    )
  }

  def join(
      on: Extractor[Any], //name is discarded
      joinType: JoinType = spooky.spookyConf.defaultJoinType,
      ordinalField: Field = null, //left & idempotent parameters are missing as they are always set to true
      sampler: Sampler[Any] = spooky.spookyConf.defaultJoinSampler
  )(
      traces: Set[Trace],
      keyBy: Trace => Any = identity,
      genPartitioner: GenPartitioner = spooky.spookyConf.defaultGenPartitioner
  ): FetchedDataset = {

    val flat = this
      .flatten(on.withJoinFieldIfMissing, joinType.isLeft, ordinalField, sampler)

    flat.fetch(traces, keyBy, genPartitioner)
  }

  /**
    * results in a new set of Pages by crawling links on old pages
    * old pages that doesn't contain the link will be ignored
    *
    * @return RDD[Page]
    */
  def visitJoin(
      on: Extractor[Any],
      joinType: JoinType = spooky.spookyConf.defaultJoinType,
      ordinalField: Field = null, //left & idempotent parameters are missing as they are always set to true
      sampler: Sampler[Any] = spooky.spookyConf.defaultJoinSampler,
      cooldown: Option[Duration] = None,
      keyBy: Trace => Any = identity,
      filter: DocFilter = Const.defaultDocumentFilter,
      failSafe: Int = -1,
      genPartitioner: GenPartitioner = spooky.spookyConf.defaultGenPartitioner
  ): FetchedDataset = {

    var trace = _defaultVisit(cooldown, filter)
    if (failSafe > 0) {
      trace = ClusterRetry(trace, failSafe)
    }

    this.join(on, joinType, ordinalField, sampler)(
      trace,
      keyBy,
      genPartitioner = genPartitioner
    )
  }

  /**
    * same as join, but avoid launching a browser by using direct http GET (wget) to download new pages
    * much faster and less stressful to both crawling and target server(s)
    *
    * @return RDD[Page]
    */
  def wgetJoin(
      on: Extractor[Any],
      joinType: JoinType = spooky.spookyConf.defaultJoinType,
      ordinalField: Field = null, //left & idempotent parameters are missing as they are always set to true
      sampler: Sampler[Any] = spooky.spookyConf.defaultJoinSampler,
      cooldown: Option[Duration] = None,
      keyBy: Trace => Any = identity,
      filter: DocFilter = Const.defaultDocumentFilter,
      failSafe: Int = -1,
      genPartitioner: GenPartitioner = spooky.spookyConf.defaultGenPartitioner
  ): FetchedDataset = {

    var trace = _defaultWget(cooldown, filter)
    if (failSafe > 0) {
      trace = ClusterRetry(trace, failSafe)
    }

    this.join(on, joinType, ordinalField, sampler)(
      trace,
      keyBy,
      genPartitioner = genPartitioner
    )
  }

  //TODO: how to unify this with join?
  def explore(
      on: Extractor[Any],
      joinType: JoinType = spooky.spookyConf.defaultJoinType,
      ordinalField: Field = null,
      sampler: Sampler[Any] = spooky.spookyConf.defaultJoinSampler
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
      //apply immediately after depth selection, this include depth0
  ): FetchedDataset = {

    val params = Params(depthField, ordinalField, range)

    ExplorePlan(
      plan,
      on.withJoinFieldIfMissing,
      sampler,
      joinType,
      traces.rewriteGlobally(plan.schema),
      keyBy,
      genPartitioner,
      params,
      exploreAlgorithm,
      epochSize,
      checkpointInterval,
      List(MapPlan.Extract(extracts))
    )
  }

  def visitExplore(
      on: Extractor[Any],
      joinType: JoinType = spooky.spookyConf.defaultJoinType,
      ordinalField: Field = null,
      sampler: Sampler[Any] = spooky.spookyConf.defaultJoinSampler,
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
      selects: Traversable[Extractor[Any]] = Seq()
  ): FetchedDataset = {

    var trace = _defaultVisit(cooldown, filter)
    if (failSafe > 0) trace = ClusterRetry(trace, failSafe)

    explore(on, joinType, ordinalField, sampler)(
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

  def wgetExplore(
      on: Extractor[Any],
      joinType: JoinType = spooky.spookyConf.defaultJoinType,
      ordinalField: Field = null,
      sampler: Sampler[Any] = spooky.spookyConf.defaultJoinSampler,
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
      selects: Traversable[Extractor[Any]] = Seq()
  ): FetchedDataset = {

    var trace = _defaultWget(cooldown, filter)
    if (failSafe > 0) trace = ClusterRetry(trace, failSafe)

    explore(on, joinType, ordinalField, sampler)(
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
