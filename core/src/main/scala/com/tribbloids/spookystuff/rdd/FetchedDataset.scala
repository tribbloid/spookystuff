package com.tribbloids.spookystuff.rdd

import com.tribbloids.spookystuff.actions.{ClusterRetry, Snapshot, Visit, Wget, _}
import com.tribbloids.spookystuff.conf.SpookyConf
import com.tribbloids.spookystuff.doc.{Doc, DocOption}
import com.tribbloids.spookystuff.dsl.{ExploreAlgorithm, JoinType, _}
import com.tribbloids.spookystuff.execution.{ExplorePlan, FetchPlan, _}
import com.tribbloids.spookystuff.extractors._
import com.tribbloids.spookystuff.extractors.impl.Get
import com.tribbloids.spookystuff.row.{Field, _}
import com.tribbloids.spookystuff.utils.SpookyViews
import com.tribbloids.spookystuff.{Const, SpookyContext}
import org.apache.spark.TaskContext
import org.apache.spark.ml.dsl.utils.refl.ScalaType
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.{CatalystTypeConverters, InternalRow}
import org.apache.spark.sql.catalyst.encoders.{ExpressionEncoder, RowEncoder}
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.types.{DataType, StructType}
import org.apache.spark.sql.utils.SparkHelper
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.storage.StorageLevel

import scala.collection.Map
import scala.collection.immutable.ListMap
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
        spooky,
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
    v =>
      new v.WSchema(schema).unsquash
  )

  def docRDD: RDD[Seq[DocOption]] = {

    squashedRDD.map {
      row =>
        new row.WSchema(schema).withSpooky.getDoc
    }
  }

  def dataRDD: RDD[DataRow] = {

    squashedRDD.flatMap {
      _.dataRows
    }
  }

  def partitionRDD = rdd.mapPartitions {
    ii =>
      Iterator(TaskContext.get().partitionId() -> ii.toSeq)
  }
  def partitionSizeRDD = rdd.mapPartitions {
    ii =>
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

    val sorted = dataRDD.sortBy{_.sortIndex(sortIndices)}
    sorted.setName("sort")

    sorted.foreachPartition{_ =>} //force execution
    plan.scratchRDDs.unpersist(dataRDD, blocking = false)

    sorted
  }
  def toMapRDD(sort: Boolean = false): RDD[Map[String, Any]] = sparkContext.withJob(s"toMapRDD(sort=$sort)"){
    {
      if (!sort) dataRDD
      else dataRDDSorted
    }
      .map(_.toMap)
  }

  def toJSON(sort: Boolean = false): RDD[String] = sparkContext.withJob(s"toJSON(sort=$sort)"){

    {
      if (!sort) dataRDD
      else dataRDDSorted
    }
      .map(_.compactJSON)
  }

  protected def toInternalRowRDD(
                                  sort: Boolean = false,
                                  spookySchema: SpookySchema
                                ): RDD[InternalRow] = {

    val dataRDD = if (!sort) this.dataRDD
    else dataRDDSorted

    import ScalaType._

//    val field2Encoder: Map[Field, ExpressionEncoder[Any]] = spookySchema.fieldTypes.mapValues {
//      tpe =>
//        val ttg = tpe.asTypeTagCasted[Any]
//        ExpressionEncoder.apply()(ttg)
//    }

    //TOOD: how to make it serializable so it can be reused by different partitions?
    @transient lazy val field2Converter: Map[Field, Any => Any] = spookySchema.fieldTypes.mapValues {
      tpe =>
        val reified = tpe.reified
        val converter = CatalystTypeConverters.createToCatalystConverter(reified)
        converter
    }

    val rowEncoder = RowEncoder.apply(spookySchema.structType)

    dataRDD
      .map {
        v =>
          val converted: Seq[Any] = spookySchema.fields.map {
            field =>
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

    unsquashedRDD.map (
      v =>
        _ex.applyOrElse[FetchedRow, String](v, _ => default)
    )
  }

  def toObjectRDD[T: ClassTag](
                                ex: Extractor[T],
                                default: T = null
                              ): RDD[T] = {

    val _ex = newResolver.include(ex).head

    unsquashedRDD.map(v => _ex.applyOrElse[FetchedRow, T](v, _ => default))
  }

  //  def toPairRDD[T1: ClassTag, T2: ClassTag](
  //                                             first: Extractor[T1],
  //                                             second: Extractor[T2]
  //                                           ): RDD[(T1,T2)] = unsquashedRDD
  //    .map {
  //      row =>
  //        val t1 = first.orNull.apply(row)
  //        val t2 = second.orNull.apply(row)
  //        t1 -> t2
  //    }

  /**
    * save each page to a designated directory
    * this is an action that will be triggered immediately
    * support many file systems including but not limited to HDFS, S3 and local HDD
    *
    * @param overwrite if a file with the same name already exist:
    *                  true: overwrite it
    *                  false: append an unique suffix to the new file name
    * @return the same RDD[Page] with file paths carried as metadata
    */
  //always use the same path pattern for filtered pages, if you want pages to be saved with different path, use multiple saveContent with different names
  def savePages(
                 path: Col[String],
                 extension: Col[String] = null,
                 page: Extractor[Doc] = S,
                 overwrite: Boolean = false
               ): this.type = {

    val effectiveExt = Option(extension).map(_.ex).getOrElse(page.defaultFileExtension)

    val _ext = newResolver.include(effectiveExt).head
    val _path = newResolver.include(path.ex).head
    val _pageExpr = newResolver.include(page).head

    //Execute immediately
    squashedRDD.foreach {
      squashedPageRow =>
        val w = new squashedPageRow
        .WSchema(schema)

        w
          .unsquash
          .foreach{
            pageRow =>
              var pathStr: Option[String] = _path.lift(pageRow).map(_.toString).map {
                str =>
                  val splitted = str.split(":")
                  if (splitted.size <= 2) str
                  else splitted.head + ":" + splitted.slice(1, Int.MaxValue).mkString("%3A") //colon in file paths are reserved for protocol definition
              }

              val extOption = _ext.lift(pageRow)
              if (extOption.nonEmpty) pathStr = pathStr.map(_ + "." + extOption.get.toString)

              pathStr.foreach {
                str =>
                  val page = _pageExpr.lift(pageRow)

                  spooky.spookyMetrics.pagesSaved += 1

                  page.foreach(_.save(Seq(str), overwrite)(spooky))
              }
          }
    }
    this
  }

  def extract[T](exs: (Extractor[T])*): FetchedDataset = {

    ExtractPlan[T](plan, exs)
  }

  def select(exprs: Extractor[_]*) = extract(exprs: _*)

  def remove(fields: Field*): FetchedDataset = RemovePlan(plan, fields)

  def removeWeaks(): FetchedDataset = this.remove(fields.filter(_.isWeak): _*)

  /**
    * extract expressions before the block and scrape all temporary KV after
    */
  //  def _extractTempDuring(exprs: Expression[Any]*)(f: PageRowRDD => PageRowRDD): PageRowRDD = {
  //
  //    val tempFields = exprs.map(_.field).filter(_.isWeak)
  //
  //    val result = f(this)
  //
  //    result.remove(tempFields: _*)
  //  }
  //
  //  def _extractInvisibleDuring(exprs: Expression[Any]*)(f: PageRowRDD => PageRowRDD): PageRowRDD = {
  //
  //    val internalFields = exprs.map(_.field).filter(_.isInvisible)
  //
  //    val result = f(this)
  //    val updateExpr = internalFields.map {
  //      field =>
  //        new GetExpr(field) ~! field.copy(isInvisible = false)
  //    }
  //
  //    result
  //      .extract(updateExpr: _*)
  //      .remove(internalFields: _*)
  //  }

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

    FlattenPlan(extracted.plan, on, ordinalField, sampler, isLeft)
  }

  //  /**
  //   * break each page into 'shards', used to extract structured data from tables
  //   * @param selector denotes enclosing elements of each shards
  //   * @param maxOrdinal only the first n elements will be used, default to Const.fetchLimit
  //   * @return RDD[Page], each page will generate several shards
  //   */
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
  def agg(exprs: Seq[(FetchedRow => Any)], reducer: RowReducer): FetchedDataset = AggPlan(plan, exprs, reducer)
  def distinctBy(exprs: (FetchedRow => Any)*): FetchedDataset = agg(exprs, (v1, v2) => v1)

  // Always left
  def fetch(
             traces: Set[Trace],
             genPartitioner: GenPartitioner = spooky.spookyConf.defaultGenPartitioner
           ): FetchedDataset = FetchPlan(plan, traces.rewriteGlobally(plan.schema), genPartitioner)

  //shorthand of fetch
  def visit(
             ex: Col[String],
             filter: DocFilter = Const.defaultDocumentFilter,
             failSafe: Int = -1,
             genPartitioner: GenPartitioner = spooky.spookyConf.defaultGenPartitioner
           ): FetchedDataset = {

    var trace: Set[Trace] =  (
      Visit(ex)
        +> Snapshot(filter)
      )
    if (failSafe > 0) trace = ClusterRetry(trace, failSafe)

    this.fetch(
      trace,
      genPartitioner = genPartitioner
    )
  }

  //shorthand of fetch
  def wget(
            ex: Col[String],
            filter: DocFilter = Const.defaultDocumentFilter,
            failSafe: Int = -1,
            genPartitioner: GenPartitioner = spooky.spookyConf.defaultGenPartitioner
          ): FetchedDataset = {

    var trace: Set[Trace] =  Wget(ex, filter)

    if (failSafe > 0) trace = ClusterRetry(trace, failSafe)

    this.fetch(
      trace,
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
            genPartitioner: GenPartitioner = spooky.spookyConf.defaultGenPartitioner
          ): FetchedDataset = {

    val flat = this
      .flatten(on.withJoinFieldIfMissing, joinType.isLeft, ordinalField, sampler)

    flat.fetch(traces, genPartitioner)
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
                 filter: DocFilter = Const.defaultDocumentFilter,
                 failSafe: Int = -1,
                 genPartitioner: GenPartitioner = spooky.spookyConf.defaultGenPartitioner
               ): FetchedDataset = {

    var trace = (
      Visit(Get(Const.defaultJoinField))
        +> Snapshot(filter)
      )
    if (failSafe > 0) {
      trace = ClusterRetry(trace, failSafe)
    }

    this.join(on, joinType, ordinalField, sampler)(
      trace,
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
                filter: DocFilter = Const.defaultDocumentFilter,
                failSafe: Int = -1,
                genPartitioner: GenPartitioner = spooky.spookyConf.defaultGenPartitioner
              ): FetchedDataset = {

    var trace: Set[Trace] = Wget(Get(Const.defaultJoinField), filter)
    if (failSafe > 0) {
      trace = ClusterRetry(trace, failSafe)
    }

    this.join(on, joinType, ordinalField, sampler)(
      trace,
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
               genPartitioner: GenPartitioner = spooky.spookyConf.defaultGenPartitioner,

               depthField: Field = null,
               range: Range = spooky.spookyConf.defaultExploreRange,
               exploreAlgorithm: ExploreAlgorithm = spooky.spookyConf.defaultExploreAlgorithm,
               epochSize: Int = spooky.spookyConf.epochSize,
               checkpointInterval: Int = spooky.spookyConf.checkpointInterval // set to Int.MaxValue to disable checkpointing,
             )(
               extracts: Extractor[Any]*
               //apply immediately after depth selection, this include depth0
             ): FetchedDataset = {

    val params = ExploreParams(depthField, ordinalField, range, extracts)

    ExplorePlan(plan, on.withJoinFieldIfMissing, sampler, joinType,
      traces.rewriteGlobally(plan.schema), genPartitioner,
      params, exploreAlgorithm, epochSize, checkpointInterval
    )
  }

  def visitExplore(
                    ex: Extractor[Any],
                    joinType: JoinType = spooky.spookyConf.defaultJoinType,
                    ordinalField: Field = null,
                    sampler: Sampler[Any] = spooky.spookyConf.defaultJoinSampler,

                    filter: DocFilter = Const.defaultDocumentFilter,

                    failSafe: Int = -1,
                    genPartitioner: GenPartitioner = spooky.spookyConf.defaultGenPartitioner,

                    depthField: Field = null,
                    range: Range = spooky.spookyConf.defaultExploreRange,
                    exploreAlgorithm: ExploreAlgorithm = spooky.spookyConf.defaultExploreAlgorithm,
                    miniBatch: Int = 500,
                    checkpointInterval: Int = spooky.spookyConf.checkpointInterval, // set to Int.MaxValue to disable checkpointing,

                    select: Extractor[Any] = null,
                    selects: Traversable[Extractor[Any]] = Seq()
                  ): FetchedDataset = {

    var trace: Set[Trace] =  (
      Visit(Get(Const.defaultJoinField))
        +> Snapshot(filter)
      )
    if (failSafe > 0) trace = ClusterRetry(trace, failSafe)

    explore(ex, joinType, ordinalField, sampler)(
      trace, genPartitioner,

      depthField, range, exploreAlgorithm, miniBatch, checkpointInterval
    )(
      Option(select).toSeq ++ selects: _*
    )
  }

  def wgetExplore(
                   ex: Extractor[Any],
                   joinType: JoinType = spooky.spookyConf.defaultJoinType,
                   ordinalField: Field = null,
                   sampler: Sampler[Any] = spooky.spookyConf.defaultJoinSampler,
                   filter: DocFilter = Const.defaultDocumentFilter,

                   failSafe: Int = -1,
                   genPartitioner: GenPartitioner = spooky.spookyConf.defaultGenPartitioner,

                   depthField: Field = null,
                   range: Range = spooky.spookyConf.defaultExploreRange,
                   exploreAlgorithm: ExploreAlgorithm = spooky.spookyConf.defaultExploreAlgorithm,
                   miniBatch: Int = 500,
                   checkpointInterval: Int = spooky.spookyConf.checkpointInterval, // set to Int.MaxValue to disable checkpointing,

                   select: Extractor[Any] = null,
                   selects: Traversable[Extractor[Any]] = Seq()
                 ): FetchedDataset = {

    var trace: Set[Trace] =  Wget(uri = Get(Const.defaultJoinField), filter = filter)
    if (failSafe > 0) trace = ClusterRetry(trace, failSafe)

    explore(ex, joinType, ordinalField, sampler)(
      trace, genPartitioner,

      depthField, range, exploreAlgorithm, miniBatch, checkpointInterval
    )(
      Option(select).toSeq ++ selects: _*
    )
  }
}