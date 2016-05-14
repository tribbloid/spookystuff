package com.tribbloids.spookystuff.rdd

import com.tribbloids.spookystuff.actions.{Snapshot, Try, Visit, Wget, _}
import com.tribbloids.spookystuff.dsl.{ExploreAlgorithm, FetchOptimizer, JoinType, _}
import com.tribbloids.spookystuff.execution.{ExplorePlan, FetchPlan, _}
import com.tribbloids.spookystuff.expressions.{GetExpr, _}
import com.tribbloids.spookystuff.doc.Doc
import com.tribbloids.spookystuff.row.{Field, _}
import com.tribbloids.spookystuff.utils.{Utils, Implicits}
import com.tribbloids.spookystuff.{Const, SpookyConf, SpookyContext}
import org.apache.spark.Partitioner
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.expressions.Alias
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.storage.StorageLevel

import scala.collection.immutable.ListSet
import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag
import scala.collection.Map

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
//TODO: rename?
case class FetchedDataset(
                       plan: ExecutionPlan
                     ) extends FetchedRDDAPI {

  import Implicits._
  import scala.Ordering.Implicits._
  import plan.CacheQueueView

  def this(
            sourceRDD: SquashedFetchedRDD,
            schema: ListSet[Field],
            spooky: SpookyContext,
            webCacheBeaconRDDOpt: Option[RDD[(Trace, DataRow)]] = None,
            cacheQueue: ArrayBuffer[RDD[_]] = ArrayBuffer()
          ) =
    this(RDDPlan(sourceRDD, schema, spooky, webCacheBeaconRDDOpt, cacheQueue))

  //TODO: use reflection for more clear API
  def setConf(f: SpookyConf => Unit): this.type = {
    f(spooky.conf)
    this
  }

  def sparkContext = plan.spooky.sparkContext
  def storageLevel: StorageLevel = plan.storageLevel
  def storageLevel_=(lv: StorageLevel) = {plan.storageLevel = lv}
  def isCached = plan.isCached

  def rdd = {
    this.spooky.rebroadcast()
    plan.rdd(true)
  }

  def unsquashedRDD: RDD[FetchedRow] = this.rdd.flatMap(_.unsquash)

  def spooky = plan.spooky
  def fields = plan.fieldSeq

  def dataRDD: RDD[DataRow] = {

    this.spooky.rebroadcast()
    plan.rdd().flatMap {
      _.dataRows
    }
  }
  def dataRDDSorted: RDD[DataRow] = {

    val sortIndexFieldsSeq: Seq[Field] = plan.sortIndexFieldSeq

    val dataRDD = this.dataRDD
    plan.cacheQueue.persist(dataRDD)

    val result = dataRDD.sortBy{_.sortIndex(sortIndexFieldsSeq)} //sort usually takes 2 passes
    result.name = "sort"

    result.foreachPartition{_ =>} //force execution
    plan.cacheQueue.unpersist(dataRDD, blocking = false)

    result
  }
  def toMapRDD(sort: Boolean = false): RDD[Map[String, Any]] = sparkContext.withJob(s"toMapRDD(sort=$sort)"){

    if (!sort) dataRDD.map(_.toMap)
    else dataRDDSorted.map(_.toMap)
  }

  def toJSON(sort: Boolean = false): RDD[String] = sparkContext.withJob(s"toJSON(sort=$sort)"){

    if (!sort) dataRDD.map(_.toJSON)
    else dataRDDSorted.map(_.toJSON)
  }

  def toDF(sort: Boolean = false, tableName: String = null): DataFrame =
    sparkContext.withJob(s"toDF(sort=$sort, name=$tableName)") {

      val jsonRDD = this.toJSON(sort)
      plan.cacheQueue.persist(jsonRDD)

      val schemaRDD = spooky.sqlContext.jsonRDD(jsonRDD)

      val columns: Seq[Column] = fields
        .filter(key => !key.isWeak)
        .map {
          key =>
            val name = Utils.canonizeColumnName(key.name)
            if (schemaRDD.schema.fieldNames.contains(name)) new Column(UnresolvedAttribute(name))
            else new Column(Alias(org.apache.spark.sql.catalyst.expressions.Literal(null), name)())
        }

      val result = schemaRDD.select(columns: _*)

      if (tableName!=null) result.registerTempTable(tableName)
      plan.cacheQueue.unpersistAll()

      result
    }

  def toStringRDD(
                   expr: Extraction[Any],
                   default: String = null
                 ): RDD[String] = unsquashedRDD.map(v => expr.toStr.applyOrElse[FetchedRow, String](v, _ => default))

  def toObjectRDD[T: ClassTag](
                                expr: Extraction[T],
                                default: T = null
                              ): RDD[T] = unsquashedRDD.map(v => expr.applyOrElse[FetchedRow, T](v, _ => default))

  def toTypedRDD[T: ClassTag](
                               expr: Extraction[Any],
                               default: T = null
                             ): RDD[T] = unsquashedRDD.map(v => expr.typed[T].applyOrElse[FetchedRow, T](v, _ => default))

  def toPairRDD[T1: ClassTag, T2: ClassTag](first: Extraction[T1], second: Extraction[T2]): RDD[(T1,T2)] = unsquashedRDD
    .map{
      row =>
        val t1: T1 = first.orNull.apply(row)
        val t2: T2 = second.orNull.apply(row)
        t1 -> t2
    }

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
                 path: Extraction[Any],
                 extension: Extraction[Any] = null,
                 pageExpr: Extraction[Doc] = S,
                 overwrite: Boolean = false
               ): this.type = {

    val effectiveExt = Option(extension).getOrElse(pageExpr.defaultExt)

    //Execute immediately
    rdd.foreach {
      squashedPageRow =>
        squashedPageRow.unsquash.foreach{
          pageRow =>
            var pathStr: Option[String] = path.lift(pageRow).map(_.toString).map {
              str =>
                val splitted = str.split(":")
                if (splitted.size <= 2) str
                else splitted.head + ":" + splitted.slice(1, Int.MaxValue).mkString("%3A") //colon in file paths are reserved for protocol definition
            }

            val extOption = effectiveExt.lift(pageRow)
            if (extOption.nonEmpty) pathStr = pathStr.map(_ + "." + extOption.get.toString)

            pathStr.foreach {
              str =>
                val page = pageExpr.lift(pageRow)

                spooky.metrics.pagesSaved += 1

                page.foreach(_.save(Seq(str), overwrite)(spooky))
            }
        }
    }
    this
  }

  def extract(exprs: Extraction[Any]*): FetchedDataset = {

    val resolvedExprs = plan.batchResolveAlias(exprs)

    this.copy(
      ExtractPlan(plan, resolvedExprs)
    )
  }

  def select(exprs: Extraction[Any]*) = extract(exprs: _*)

  def remove(fields: Field*): FetchedDataset = this.copy(RemovePlan(plan, fields))

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
               expr: Extraction[Any],
               isLeft: Boolean = true,
               ordinalField: Field = null,
               sampler: Sampler[Any] = spooky.conf.defaultFlattenSampler
             ): FetchedDataset = {

    val extracted = if  (expr.isInstanceOf[GetExpr]) this
    else this.extract(expr)

    val resolvedExpr = extracted.plan.resolveAlias(expr)

    val effectiveOrdinalField = Option(ordinalField) match {
      case Some(ff) =>
        ff.copy(isOrdinal = true)
      case None =>
        Field(resolvedExpr.field.name + "_ordinal", isWeak = true, isOrdinal = true)
    }

    this.copy(
      FlattenPlan(extracted.plan, resolvedExpr.field, effectiveOrdinalField, sampler, isLeft)
    )
  }

  //  /**
  //   * break each page into 'shards', used to extract structured data from tables
  //   * @param selector denotes enclosing elements of each shards
  //   * @param maxOrdinal only the first n elements will be used, default to Const.fetchLimit
  //   * @return RDD[Page], each page will generate several shards
  //   */
  def flatExtract(
                   expr: Extraction[Any], //TODO: used to be Iterable[Unstructured], any tradeoff?
                   isLeft: Boolean = true,
                   ordinalField: Field = null,
                   sampler: Sampler[Any] = spooky.conf.defaultFlattenSampler
                 )(exprs: Extraction[Any]*): FetchedDataset = {
    this
      .flatten(expr defaultAs Const.defaultJoinField, isLeft, ordinalField, sampler)
      .extract(exprs: _*)
  }

  def flatSelect(
                  expr: Extraction[Any], //TODO: used to be Iterable[Unstructured], any tradeoff?
                  ordinalField: Field = null,
                  sampler: Sampler[Any] = spooky.conf.defaultFlattenSampler,
                  isLeft: Boolean = true
                )(exprs: Extraction[Any]*) = flatExtract(expr, isLeft, ordinalField, sampler)(exprs: _*)

  //TODO: test
  def agg(exprs: Seq[(FetchedRow => Any)], reducer: RowReducer): FetchedDataset = this.copy(AggPlan(plan, exprs, reducer))
  def distinctBy(exprs: (FetchedRow => Any)*): FetchedDataset = agg(exprs, (v1, v2) => v1)

  // Always left
  def fetch(
             traces: Set[Trace],
             partitionerFactory: RDD[_] => Partitioner = spooky.conf.defaultPartitionerFactory,
             fetchOptimizer: FetchOptimizer = spooky.conf.defaultFetchOptimizer
           ): FetchedDataset = this.copy(FetchPlan(plan, traces.correct, partitionerFactory, fetchOptimizer))

  //shorthand of fetch
  def visit(
             expr: Extraction[Any],
             filter: DocFilter = Const.defaultDocumentFilter,
             failSafe: Int = -1,
             partitionerFactory: RDD[_] => Partitioner = spooky.conf.defaultPartitionerFactory,
             fetchOptimizer: FetchOptimizer = spooky.conf.defaultFetchOptimizer
           ): FetchedDataset = {

    var trace: Set[Trace] =  (
      Visit(expr)
        +> Snapshot(filter)
      )
    if (failSafe > 0) trace = Try(trace, failSafe)

    this.fetch(
      trace,
      partitionerFactory = partitionerFactory,
      fetchOptimizer = fetchOptimizer
    )
  }

  //shorthand of fetch
  def wget(
            expr: Extraction[Any],
            filter: DocFilter = Const.defaultDocumentFilter,
            failSafe: Int = -1,
            partitionerFactory: RDD[_] => Partitioner = spooky.conf.defaultPartitionerFactory,
            fetchOptimizer: FetchOptimizer = spooky.conf.defaultFetchOptimizer
          ): FetchedDataset = {

    var trace: Set[Trace] =  Wget(expr, filter)

    if (failSafe > 0) trace = Try(trace, failSafe)

    this.fetch(
      trace,
      partitionerFactory = partitionerFactory,
      fetchOptimizer = fetchOptimizer
    )
  }

  def join(
            expr: Extraction[Any], //name is discarded
            joinType: JoinType = spooky.conf.defaultJoinType,
            ordinalField: Field = null, //left & idempotent parameters are missing as they are always set to true
            sampler: Sampler[Any] = spooky.conf.defaultJoinSampler
          )(
            traces: Set[Trace],
            partitionerFactory: RDD[_] => Partitioner = spooky.conf.defaultPartitionerFactory,
            fetchOptimizer: FetchOptimizer = spooky.conf.defaultFetchOptimizer
          ): FetchedDataset = {

    val flat = this
      .flatten(expr defaultAs Const.defaultJoinField, joinType.isLeft, ordinalField, sampler)

    flat.fetch(traces, partitionerFactory, fetchOptimizer)
  }

  /**
    * results in a new set of Pages by crawling links on old pages
    * old pages that doesn't contain the link will be ignored
    *
    * @return RDD[Page]
    */
  def visitJoin(
                 expr: Extraction[Any],
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
      trace = Try(trace, failSafe)
    }

    this.join(expr, joinType, ordinalField, sampler)(
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
                expr: Extraction[Any],
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
      trace = Try(trace, failSafe)
    }

    this.join(expr, joinType, ordinalField, sampler)(
      trace,
      partitionerFactory,
      fetchOptimizer = fetchOptimizer
    )
  }

  //TODO: how to unify this with join?
  def explore(
               expr: Extraction[Any],
               joinType: JoinType = spooky.conf.defaultJoinType, //TODO: should be hardcoded to Inner, but whatever...
               ordinalField: Field = null,
               sampler: Sampler[Any] = spooky.conf.defaultJoinSampler
             )(
               traces: Set[Trace],
               partitionerFactory: RDD[_] => Partitioner = spooky.conf.defaultPartitionerFactory,
               fetchOptimizer: FetchOptimizer = spooky.conf.defaultFetchOptimizer,

               depthField: Field = null,
               range: Range = spooky.conf.defaultExploreRange,
               exploreAlgorithm: ExploreAlgorithm = spooky.conf.defaultExploreAlgorithm,
               miniBatch: Int = 500,
               checkpointInterval: Int = spooky.conf.checkpointInterval // set to Int.MaxValue to disable checkpointing,
             )(
               extracts: Extraction[Any]*
               //apply immediately after depth selection, this include depth0
             ): FetchedDataset = {

    val resolvedExpr = plan.resolveAlias(expr defaultAs Const.defaultJoinField)
    val resolvedExtracts = plan.batchResolveAlias(extracts)

    val effectiveOrdinalField = Option(ordinalField) match {
      case Some(ff) =>
        ff.copy(isOrdinal = true)
      case None =>
        Field(resolvedExpr.field.name + "_ordinal", isWeak = true, isOrdinal = true)
    }

    val effectiveDepthField = Option(depthField) match {
      case Some(field) =>
        val resolvedField = field.resolveConflict(plan.schema)
        resolvedField.copy(depthRangeOpt = Some(range))
      case None =>
        Field(resolvedExpr.field.name + "_depth", isWeak = true, depthRangeOpt = Some(range))
    }

    val algorithmImpl = exploreAlgorithm.getImpl(effectiveDepthField, effectiveOrdinalField, resolvedExtracts)

    this.copy(
      ExplorePlan(plan, resolvedExpr, sampler, joinType,
        traces.correct, partitionerFactory, fetchOptimizer,
        algorithmImpl, miniBatch, checkpointInterval
      )
    )
  }

  def visitExplore(
                    expr: Extraction[Any],
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

                    select: Extraction[Any] = null,
                    selects: Traversable[Extraction[Any]] = Seq()
                  ): FetchedDataset = {

    var trace: Set[Trace] =  (
      Visit(new GetExpr(Const.defaultJoinField))
        +> Snapshot(filter)
      )
    if (failSafe > 0) trace = Try(trace, failSafe)

    explore(expr, joinType, ordinalField, sampler)(
      trace, partitionerFactory, fetchOptimizer,

      depthField, range, exploreAlgorithm, miniBatch, checkpointInterval
    )(
      Option(select).toSeq ++ selects: _*
    )
  }

  def wgetExplore(
                   expr: Extraction[Any],
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

                   select: Extraction[Any] = null,
                   selects: Traversable[Extraction[Any]] = Seq()
                 ): FetchedDataset = {

    var trace: Set[Trace] =  Wget(new GetExpr(Const.defaultJoinField), filter)
    if (failSafe > 0) trace = Try(trace, failSafe)

    explore(expr, joinType, ordinalField, sampler)(
      trace, partitionerFactory, fetchOptimizer,

      depthField, range, exploreAlgorithm, miniBatch, checkpointInterval
    )(
      Option(select).toSeq ++ selects: _*
    )
  }
}