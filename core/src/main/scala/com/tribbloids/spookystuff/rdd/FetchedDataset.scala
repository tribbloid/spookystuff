package com.tribbloids.spookystuff.rdd

import com.tribbloids.spookystuff.actions.{ClusterRetry, Snapshot, Visit, Wget, _}
import com.tribbloids.spookystuff.doc.Doc
import com.tribbloids.spookystuff.dsl.{ExploreAlgorithm, FetchOptimizer, JoinType, _}
import com.tribbloids.spookystuff.execution.{ExplorePlan, FetchPlan, _}
import com.tribbloids.spookystuff.extractors.{GetExpr, _}
import com.tribbloids.spookystuff.row.{Field, _}
import com.tribbloids.spookystuff.utils.{SpookyUtils, SpookyViews}
import com.tribbloids.spookystuff.{Const, SpookyConf, SpookyContext}
import org.apache.spark.Partitioner
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.expressions
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.{Column, DataFrame, Row}
import org.apache.spark.storage.StorageLevel

import scala.collection.Map
import scala.collection.immutable.ListMap
import scala.collection.mutable.ArrayBuffer
import scala.language.implicitConversions
import scala.reflect.ClassTag

import scala.Ordering.Implicits._

/**
  * Created by peng on 28/03/16.
  */
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
  import plan.CacheQueueView

  implicit def plan2Dataset(plan: ExecutionPlan): FetchedDataset = FetchedDataset(plan)

  def this(
            sourceRDD: SquashedFetchedRDD,
            fieldMap: ListMap[Field, DataType],
            spooky: SpookyContext,
            beaconRDDOpt: Option[RDD[(TraceView, DataRow)]] = None,
            cacheQueue: ArrayBuffer[RDD[_]] = ArrayBuffer()
          ) = {

    this(RDDPlan(sourceRDD, DataRowSchema(spooky, fieldMap), spooky, beaconRDDOpt, cacheQueue))
  }

  //TODO: use reflection for more clear API
  def setConf(f: SpookyConf => Unit): this.type = {
    f(spooky.conf)
    this
  }

  def sparkContext = plan.spooky.sparkContext
  def storageLevel: StorageLevel = plan.storageLevel
  def storageLevel_=(lv: StorageLevel) = {plan.storageLevel = lv}
  def isCached = plan.isCached

  def squashedRDD = {
    this.spooky.rebroadcast()
    plan.rdd()
  }

  def rdd = unsquashedRDD

  def unsquashedRDD: RDD[FetchedRow] = this.squashedRDD.flatMap(v => new v.WithSchema(schema).unsquash)

  def spooky = plan.spooky
  def schema = plan.schema
  def fields = schema.fields

  def dataRDD: RDD[DataRow] = {

    this.spooky.rebroadcast()
    plan.rdd().flatMap {
      _.dataRows
    }
  }

  def dataRDDSorted: RDD[DataRow] = {

    val sortIndices: List[Field] = plan.allSortIndices.map(_._1.self)

    val dataRDD = this.dataRDD
    plan.cacheQueue.persist(dataRDD)

    val sorted = dataRDD.sortBy{_.sortIndex(sortIndices)} //sort usually takes 2 passes
    sorted.name = "sort"

    sorted.foreachPartition{_ =>} //force execution
    plan.cacheQueue.unpersist(dataRDD, blocking = false)

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

  //TODO: take filter schema as parameter
  protected def toRowRDD(sort: Boolean = false, fields: List[Field]): RDD[Row] = {

    val dataRDD = if (!sort) this.dataRDD
    else dataRDDSorted

    dataRDD
      .map {
        v =>
          val result = Row(fields.map(vv => v.data.get(vv).orNull): _*)
          result
      }
  }

  def toDF(sort: Boolean = false, tableName: String = null): DataFrame =
    sparkContext.withJob(s"toDF(sort=$sort, name=$tableName)") {

      val filtered = schema.filterFields()

      val structType = filtered.toStructType

      val rowRDD = toRowRDD(sort, filtered.map.keys.toList)

      val result = spooky.sqlContext.createDataFrame(rowRDD, structType)

      if (tableName!=null) result.registerTempTable(tableName)

      result
    }

  //TODO: cleanup, useful only in comparison
  def toDFLegacy(sort: Boolean = false, tableName: String = null): DataFrame =
    sparkContext.withJob(s"toDF(sort=$sort, name=$tableName)") {

      val jsonRDD = this.toJSON(sort)
      plan.cacheQueue.persist(jsonRDD)

      val schemaRDD = spooky.sqlContext.read.json(jsonRDD)

      val columns: Seq[Column] = fields
        .filter(key => !key.isWeak)
        .map {
          key =>
            val name = SpookyUtils.canonizeColumnName(key.name)
            if (schemaRDD.schema.fieldNames.contains(name)) new Column(UnresolvedAttribute(name))
            else new Column(expressions.Alias(org.apache.spark.sql.catalyst.expressions.Literal(null), name)())
        }

      val result = schemaRDD.select(columns: _*)

      if (tableName!=null) result.registerTempTable(tableName)
      plan.cacheQueue.unpersistAll()

      result
    }

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
                 path: Extractor[Any],
                 extension: Extractor[Any] = null,
                 page: Extractor[Doc] = S,
                 overwrite: Boolean = false
               ): this.type = {

    val effectiveExt = Option(extension).getOrElse(page.defaultFileExtension)

    val _ext = newResolver.include(effectiveExt).head
    val _path = newResolver.include(path).head
    val _pageExpr = newResolver.include(page).head

    //Execute immediately
    squashedRDD.foreach {
      squashedPageRow =>
        val w = new squashedPageRow
        .WithSchema(schema)

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

                  spooky.metrics.pagesSaved += 1

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
               sampler: Sampler[Any] = spooky.conf.defaultFlattenSampler
             ): FetchedDataset = {

    val (on, extracted) = ex match {
      case GetExpr(ff) =>
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
                   sampler: Sampler[Any] = spooky.conf.defaultFlattenSampler
                 )(exprs: Extractor[Any]*): FetchedDataset = {
    this
      .flatten(on.withJoinFieldIfMissing, isLeft, ordinalField, sampler)
      .extract(exprs: _*)
  }

  def flatSelect(
                  on: Extractor[Any], //TODO: used to be Iterable[Unstructured], any tradeoff?
                  ordinalField: Field = null,
                  sampler: Sampler[Any] = spooky.conf.defaultFlattenSampler,
                  isLeft: Boolean = true
                )(exprs: Extractor[Any]*) = flatExtract(on, isLeft, ordinalField, sampler)(exprs: _*)

  //TODO: test
  def agg(exprs: Seq[(FetchedRow => Any)], reducer: RowReducer): FetchedDataset = AggPlan(plan, exprs, reducer)
  def distinctBy(exprs: (FetchedRow => Any)*): FetchedDataset = agg(exprs, (v1, v2) => v1)

  // Always left
  def fetch(
             traces: Set[Trace],
             partitionerFactory: RDD[_] => Partitioner = spooky.conf.defaultPartitionerFactory,
             fetchOptimizer: FetchOptimizer = spooky.conf.defaultFetchOptimizer
           ): FetchedDataset = FetchPlan(plan, traces.correct, partitionerFactory, fetchOptimizer)

  //shorthand of fetch
  def visit(
             ex: Extractor[Any],
             filter: DocFilter = Const.defaultDocumentFilter,
             failSafe: Int = -1,
             partitionerFactory: RDD[_] => Partitioner = spooky.conf.defaultPartitionerFactory,
             fetchOptimizer: FetchOptimizer = spooky.conf.defaultFetchOptimizer
           ): FetchedDataset = {

    var trace: Set[Trace] =  (
      Visit(ex)
        +> Snapshot(filter)
      )
    if (failSafe > 0) trace = ClusterRetry(trace, failSafe)

    this.fetch(
      trace,
      partitionerFactory = partitionerFactory,
      fetchOptimizer = fetchOptimizer
    )
  }

  //shorthand of fetch
  def wget(
            ex: Extractor[Any],
            filter: DocFilter = Const.defaultDocumentFilter,
            failSafe: Int = -1,
            partitionerFactory: RDD[_] => Partitioner = spooky.conf.defaultPartitionerFactory,
            fetchOptimizer: FetchOptimizer = spooky.conf.defaultFetchOptimizer
          ): FetchedDataset = {

    var trace: Set[Trace] =  Wget(ex, filter)

    if (failSafe > 0) trace = ClusterRetry(trace, failSafe)

    this.fetch(
      trace,
      partitionerFactory = partitionerFactory,
      fetchOptimizer = fetchOptimizer
    )
  }

  def join(
            on: Extractor[Any], //name is discarded
            joinType: JoinType = spooky.conf.defaultJoinType,
            ordinalField: Field = null, //left & idempotent parameters are missing as they are always set to true
            sampler: Sampler[Any] = spooky.conf.defaultJoinSampler
          )(
            traces: Set[Trace],
            partitionerFactory: RDD[_] => Partitioner = spooky.conf.defaultPartitionerFactory,
            fetchOptimizer: FetchOptimizer = spooky.conf.defaultFetchOptimizer
          ): FetchedDataset = {

    val flat = this
      .flatten(on.withJoinFieldIfMissing, joinType.isLeft, ordinalField, sampler)

    flat.fetch(traces, partitionerFactory, fetchOptimizer)
  }

  /**
    * results in a new set of Pages by crawling links on old pages
    * old pages that doesn't contain the link will be ignored
    *
    * @return RDD[Page]
    */
  def visitJoin(
                 on: Extractor[Any],
                 joinType: JoinType = spooky.conf.defaultJoinType,
                 ordinalField: Field = null, //left & idempotent parameters are missing as they are always set to true
                 sampler: Sampler[Any] = spooky.conf.defaultJoinSampler,
                 filter: DocFilter = Const.defaultDocumentFilter,
                 failSafe: Int = -1,
                 partitionerFactory: RDD[_] => Partitioner = spooky.conf.defaultPartitionerFactory,
                 fetchOptimizer: FetchOptimizer = spooky.conf.defaultFetchOptimizer
               ): FetchedDataset = {

    var trace = (
      Visit(new GetExpr(Const.defaultJoinField))
        +> Snapshot(filter)
      )
    if (failSafe > 0) {
      trace = ClusterRetry(trace, failSafe)
    }

    this.join(on, joinType, ordinalField, sampler)(
      trace,
      partitionerFactory,
      fetchOptimizer = fetchOptimizer
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
                joinType: JoinType = spooky.conf.defaultJoinType,
                ordinalField: Field = null, //left & idempotent parameters are missing as they are always set to true
                sampler: Sampler[Any] = spooky.conf.defaultJoinSampler,
                filter: DocFilter = Const.defaultDocumentFilter,
                failSafe: Int = -1,
                partitionerFactory: RDD[_] => Partitioner = spooky.conf.defaultPartitionerFactory,
                fetchOptimizer: FetchOptimizer = spooky.conf.defaultFetchOptimizer
              ): FetchedDataset = {

    var trace: Set[Trace] = Wget(new GetExpr(Const.defaultJoinField), filter)
    if (failSafe > 0) {
      trace = ClusterRetry(trace, failSafe)
    }

    this.join(on, joinType, ordinalField, sampler)(
      trace,
      partitionerFactory,
      fetchOptimizer = fetchOptimizer
    )
  }

  //TODO: how to unify this with join?
  def explore(
               on: Extractor[Any],
               joinType: JoinType = spooky.conf.defaultJoinType,
               ordinalField: Field = null,
               sampler: Sampler[Any] = spooky.conf.defaultJoinSampler
             )(
               traces: Set[Trace],
               partitionerFactory: RDD[_] => Partitioner = spooky.conf.defaultPartitionerFactory,
               fetchOptimizer: FetchOptimizer = spooky.conf.defaultFetchOptimizer,

               depthField: Field = null,
               range: Range = spooky.conf.defaultExploreRange,
               exploreAlgorithm: ExploreAlgorithm = spooky.conf.defaultExploreAlgorithm,
               epochSize: Int = spooky.conf.epochSize,
               checkpointInterval: Int = spooky.conf.checkpointInterval // set to Int.MaxValue to disable checkpointing,
             )(
               extracts: Extractor[Any]*
               //apply immediately after depth selection, this include depth0
             ): FetchedDataset = {

    val params = ExploreParams(depthField, ordinalField, range, extracts)

    ExplorePlan(plan, on.withJoinFieldIfMissing, sampler, joinType,
      traces.correct, partitionerFactory, fetchOptimizer,
      params, exploreAlgorithm, epochSize, checkpointInterval
    )
  }

  def visitExplore(
                    ex: Extractor[Any],
                    joinType: JoinType = spooky.conf.defaultJoinType,
                    ordinalField: Field = null,
                    sampler: Sampler[Any] = spooky.conf.defaultJoinSampler,

                    filter: DocFilter = Const.defaultDocumentFilter,

                    failSafe: Int = -1,
                    partitionerFactory: RDD[_] => Partitioner = spooky.conf.defaultPartitionerFactory,
                    fetchOptimizer: FetchOptimizer = spooky.conf.defaultFetchOptimizer,

                    depthField: Field = null,
                    range: Range = spooky.conf.defaultExploreRange,
                    exploreAlgorithm: ExploreAlgorithm = spooky.conf.defaultExploreAlgorithm,
                    miniBatch: Int = 500,
                    checkpointInterval: Int = spooky.conf.checkpointInterval, // set to Int.MaxValue to disable checkpointing,

                    select: Extractor[Any] = null,
                    selects: Traversable[Extractor[Any]] = Seq()
                  ): FetchedDataset = {

    var trace: Set[Trace] =  (
      Visit(new GetExpr(Const.defaultJoinField))
        +> Snapshot(filter)
      )
    if (failSafe > 0) trace = ClusterRetry(trace, failSafe)

    explore(ex, joinType, ordinalField, sampler)(
      trace, partitionerFactory, fetchOptimizer,

      depthField, range, exploreAlgorithm, miniBatch, checkpointInterval
    )(
      Option(select).toSeq ++ selects: _*
    )
  }

  def wgetExplore(
                   ex: Extractor[Any],
                   joinType: JoinType = spooky.conf.defaultJoinType,
                   ordinalField: Field = null,
                   sampler: Sampler[Any] = spooky.conf.defaultJoinSampler,
                   filter: DocFilter = Const.defaultDocumentFilter,

                   failSafe: Int = -1,
                   partitionerFactory: RDD[_] => Partitioner = spooky.conf.defaultPartitionerFactory,
                   fetchOptimizer: FetchOptimizer = spooky.conf.defaultFetchOptimizer,

                   depthField: Field = null,
                   range: Range = spooky.conf.defaultExploreRange,
                   exploreAlgorithm: ExploreAlgorithm = spooky.conf.defaultExploreAlgorithm,
                   miniBatch: Int = 500,
                   checkpointInterval: Int = spooky.conf.checkpointInterval, // set to Int.MaxValue to disable checkpointing,

                   select: Extractor[Any] = null,
                   selects: Traversable[Extractor[Any]] = Seq()
                 ): FetchedDataset = {

    var trace: Set[Trace] =  Wget(new GetExpr(Const.defaultJoinField), filter)
    if (failSafe > 0) trace = ClusterRetry(trace, failSafe)

    explore(ex, joinType, ordinalField, sampler)(
      trace, partitionerFactory, fetchOptimizer,

      depthField, range, exploreAlgorithm, miniBatch, checkpointInterval
    )(
      Option(select).toSeq ++ selects: _*
    )
  }
}