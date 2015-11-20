package com.tribbloids.spookystuff.sparkbinding

import com.tribbloids.spookystuff._
import com.tribbloids.spookystuff.actions._
import com.tribbloids.spookystuff.dsl._
import com.tribbloids.spookystuff.expressions._
import com.tribbloids.spookystuff.pages.{Page, PageLike, Unstructured}
import com.tribbloids.spookystuff.row._
import com.tribbloids.spookystuff.utils._
import org.apache.spark.rdd.{RDD, UnionRDD}
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.expressions.Alias
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{HashPartitioner, Partitioner}
import org.slf4j.LoggerFactory

import scala.collection.immutable.ListSet
import scala.collection.mutable.ArrayBuffer
import scala.language.implicitConversions
import scala.reflect.ClassTag
import scala.util.Random

/**
 * Created by peng on 8/29/14.
 * Core component, abstraction of distributed Page + schemaless KVStore to represent all stages of remote resource discovery
 */
class PageRowRDD private (
                           val selfRDD: RDD[PageRow],
                           val webCacheRDD: WebCacheRDD, //in Memory cache, used for both quick page lookup from memory and seeding
                           val keys: ListSet[KeyLike],
                           val spooky: SpookyContext,
                           val persistedTempRDDs: ArrayBuffer[RDD[_]]
                           )
  extends PageRowRDDApi {

  import RDD._
  import Views._

  def this(
            selfRDD: RDD[PageRow],
            spooky: SpookyContext
            ) = this(

    selfRDD,
    spooky.sparkContext.emptyRDD[WebCacheRow].partitionBy(new HashPartitioner(spooky.conf.defaultParallelism(selfRDD))),
    ListSet(), spooky, ArrayBuffer())

  def this(
            selfRDD: RDD[PageRow],
            keys: ListSet[KeyLike],
            spooky: SpookyContext
            ) = this(

    selfRDD,
    spooky.sparkContext.emptyRDD[WebCacheRow].partitionBy(new HashPartitioner(spooky.conf.defaultParallelism(selfRDD))),
    keys, spooky, ArrayBuffer())

  def copy(
            selfRDD: RDD[PageRow] = this.selfRDD,
            webCache: WebCacheRDD = this.webCacheRDD,
            keys: ListSet[KeyLike] = this.keys,
            spooky: SpookyContext = this.spooky,
            persisted: ArrayBuffer[RDD[_]] = this.persistedTempRDDs
            ): PageRowRDD = {

    val result = new PageRowRDD(
      selfRDD,
      webCache,
      keys,
      spooky,
      persisted
    )
    result
  }

  def sparkContext = this.selfRDD.sparkContext

  def setConf(f: SpookyConf => Unit): PageRowRDD = {
    f(this.spooky.conf)
    this
  }

  //create a web cache RDD from scratch
  //equivalent to empty WebCacheRDD.putRows(this) defined in Views.
  def selfRDDToWebCacheRDD(
                            seedFilter: Iterable[PageRow] => Option[PageRow] = v => v.headOption,
                            partitionerOption: Option[Partitioner] = None
                            ): WebCacheRDD = {

    val dryRun_RowRDD = this.keyBy(_.dryrun)
    val grouped = partitionerOption match {
      case Some(partitioner) => dryRun_RowRDD.groupByKey(partitioner)
      case None => dryRun_RowRDD.groupByKey()
    }
    val result = grouped.map {
      (dryrun_rows: (List[Trace], Iterable[PageRow])) =>
        val newRows = dryrun_rows._2


        val seedRows = newRows
          .groupBy(_.uid)
          .flatMap(tuple => seedFilter(tuple._2))

        val newPageLikes = seedRows.head.pageLikes //all newRows should have identical pageLikes (if using wide optimizer)

        val newCached = dryrun_rows._1 -> Squashed(pageLikes = newPageLikes, rows = seedRows.toArray)
        newCached
    }
    result
  }

  //remove rows that yields identical results given the function(s)
  def distinctBy(exprs: (PageRow => Any)*): PageRowRDD = {
    this.copy(selfRDD.groupBy {
      row =>
        exprs.map(_.apply(row))
    }.flatMap {
      tuple =>
        tuple._2.headOption
    })
  }

  //segment rows by results of the given function(s)
  def segmentBy(exprs: (PageRow => Any)*): PageRowRDD = {

    this.copy(selfRDD.map{
      row =>
        val values = exprs.map(_.apply(row))
        row.copy(segmentID = values.hashCode())
    })
  }

  def segmentByRow: PageRowRDD = {
    this.copy(selfRDD.map(_.copy(segmentID = Random.nextLong())))
  }

  private def discardPages: PageRowRDD = this.copy(selfRDD = selfRDD.map(_.copy(pageLikes = Array())))

  private[sparkbinding] def discardDataRowsInWebCacheRDD: PageRowRDD = {
    this.copy(webCache = webCacheRDD.discardDataRows)
  }

  private def persistTemp(rdd: RDD[_]): PageRowRDD = {
    if (this.getStorageLevel == StorageLevel.NONE) {
      this.persistedTempRDDs += rdd.persist(spooky.conf.defaultStorageLevel)
    }
    this
  }

  private def unpersistAllTemp(): PageRowRDD = {
    this.persistedTempRDDs.foreach(_.unpersist(blocking = false))
    this.persistedTempRDDs.clear()
    this
  }

  @transient def keySeq: Seq[KeyLike] = this.keys.toSeq.reverse

  @transient def sortKeysSeq: Seq[SortKey] = keySeq.flatMap{
    case k: SortKey => Some(k)
    case _ => None
  }

  private def defaultSort: PageRowRDD = {

    val sortKeysSeq = this.sortKeysSeq

    import scala.Ordering.Implicits._

    this.persistTemp(this.selfRDD)

    val result = this.sortBy{_.ordinal(sortKeysSeq)} //sort usually takes 2 passes
    result.foreachPartition{_ =>}
    this.unpersistAllTemp()
    result.name = "sort"
    result
  }

  def toMapRDD(sort: Boolean = false): RDD[Map[String, Any]] = sparkContext.withJob(s"toMapRDD(sort=$sort)"){

    if (!sort) this.map(_.toMap)
    else this
      .discardPages
      .defaultSort
      .map(_.toMap)
  }

  def toJSON(sort: Boolean = false): RDD[String] = sparkContext.withJob(s"toJSON(sort=$sort)"){

    if (!sort) this.map(_.toJSON)
    else this
      .discardPages
      .defaultSort
      .map(_.toJSON)
  }

  def toDF(sort: Boolean = false, name: String = null): DataFrame = sparkContext.withJob(s"toDF(sort=$sort, name=$name)"){

    val jsonRDD = this.persist().toJSON(sort) //can't persistTemp as jsonRDD itself cannot be immediately cleaned

    val schemaRDD = this.spooky.sqlContext.jsonRDD(jsonRDD)

    val columns: Seq[Column] = keySeq
      .filter(key => key.isInstanceOf[Key])
      .map {
        key =>
          val name = Utils.canonizeColumnName(key.name)
          if (schemaRDD.schema.fieldNames.contains(name)) new Column(UnresolvedAttribute(name))
          else new Column(Alias(org.apache.spark.sql.catalyst.expressions.Literal(null), name)())
      }

    val result = schemaRDD.select(columns: _*)

    if (name!=null) result.registerTempTable(name)
    this.unpersistAllTemp()

    result
  }

  def toStringRDD(expr: Expression[Any]): RDD[String] = this.map(expr.toStr.orNull)

  def toObjectRDD[T: ClassTag](expr: Expression[T]): RDD[T] = this.map(expr.orNull)

  def toTypedRDD[T: ClassTag](expr: Expression[Any]): RDD[T] = this.map(expr.typed[T].orNull)

  def toPairRDD[T1: ClassTag, T2: ClassTag](first: Expression[T1], second: Expression[T2]): RDD[(T1,T2)] = this.map{
    row =>
      val t1: T1 = first.orNull.apply(row)
      val t2: T2 = second.orNull.apply(row)
      t1 -> t2
  }

  /**
   * save each page to a designated directory
   * this is an action that will be triggered immediately
   * support many file systems including but not limited to HDFS, S3 and local HDD
   * @param overwrite if a file with the same name already exist:
   *                  true: overwrite it
   *                  false: append an unique suffix to the new file name
   * @return the same RDD[Page] with file paths carried as metadata
   */
  //always use the same path pattern for filtered pages, if you want pages to be saved with different path, use multiple saveContent with different names
  def savePages(
                 path: Expression[Any],
                 ext: Expression[Any] = null,
                 pageExpr: Expression[Page] = S,
                 overwrite: Boolean = false //TODO: move to context & more option
                 ): PageRowRDD = Operations.SavePages(path, ext, pageExpr, overwrite).apply(this)

  //  /**
  //   * extract parts of each Page and insert into their respective context
  //   * if a key already exist in old context it will be replaced with the new one.
  //   * @param exprs
  //   * @return new PageRowRDD
  //   */
  def select(exprs: Expression[Any]*): PageRowRDD = Operations.Select(exprs: _*).apply(this)

  //alias
  def extract(exprs: Expression[Any]*) = select(exprs: _*)

  //bypass "already exist" check
  def select_!(exprs: Expression[Any]*): PageRowRDD = Operations.Select_!(exprs: _*).apply(this)

  def extract_!(exprs: Expression[Any]*) = select_!(exprs: _*)

  private def selectTemp(exprs: Expression[Any]*): PageRowRDD = {

    val newKeys: Seq[TempKey] = exprs.flatMap {
      expr =>
        if (expr.name == null) None
        else {
          val key = TempKey(expr.name)
          Some(key)
        }
    }

    this.copy(
      selfRDD = this.flatMap(_.selectTemp(exprs: _*)),
      keys = this.keys ++ newKeys
    )
  }

  def remove(keys: Symbol*): PageRowRDD = Operations.Remove(keys: _*).apply(this)

  // alias
  def deselect(keys: Symbol*) = remove(keys: _*)

  private[sparkbinding] def clearTempData: PageRowRDD = {
    this.copy(
      selfRDD = this.map(_.clearTempData),
      keys = keys -- keys.filter(_.isInstanceOf[TempKey])//circumvent https://issues.scala-lang.org/browse/SI-8985
    )
  }

  def flatten(
               expr: Expression[Any],
               ordinalKey: Symbol = null,
               maxOrdinal: Int = Int.MaxValue,
               left: Boolean = true
               ): PageRowRDD = Operations.Flatten(expr, ordinalKey, maxOrdinal, left).apply(this)

  private[sparkbinding] def flattenTemp(
                                         expr: Expression[Any],
                                         ordinalKey: Symbol = null,
                                         maxOrdinal: Int = Int.MaxValue,
                                         left: Boolean = true
                                         ): PageRowRDD = {
    val selected = this.selectTemp(expr)

    val flattened = selected.flatMap(_.flatten(expr.name, ordinalKey, maxOrdinal, left))
    selected.copy(
      selfRDD = flattened,
      keys = selected.keys ++ Option(Key.ordinalKey(ordinalKey))
    )
  }

  //alias of flatten
  def explode(
               expr: Expression[Any],
               ordinalKey: Symbol = null,
               maxOrdinal: Int = Int.MaxValue,
               left: Boolean = true
               ): PageRowRDD = flatten(expr, ordinalKey, maxOrdinal, left)

  //  /**
  //   * break each page into 'shards', used to extract structured data from tables
  //   * @param selector denotes enclosing elements of each shards
  //   * @param maxOrdinal only the first n elements will be used, default to Const.fetchLimit
  //   * @return RDD[Page], each page will generate several shards
  //   */
  def flatSelect(
                  expr: Expression[Iterable[Unstructured]], //avoid confusion
                  ordinalKey: Symbol = null,
                  maxOrdinal: Int = Int.MaxValue,
                  left: Boolean = true
                  )(exprs: Expression[Any]*) = Operations.FlatSelect(expr, ordinalKey, maxOrdinal, left)(exprs: _*).apply(this)

  //alias
  def flatExtract(
                   expr: Expression[Iterable[Unstructured]],
                   ordinalKey: Symbol = null,
                   maxOrdinal: Int = Int.MaxValue,
                   left: Boolean = true
                   )(exprs: Expression[Any]*) = flatSelect(expr, ordinalKey, maxOrdinal, left)(exprs: _*)

  private[sparkbinding] def flattenPages(
                                          pattern: String = ".*", //TODO: enable it, or remvoe all together
                                          ordinalKey: Symbol = null
                                          ): PageRowRDD =

    this.copy(
      selfRDD = this.flatMap(_.flattenPages(pattern.name, ordinalKey)),
      keys = this.keys ++ Option(Key.ordinalKey(ordinalKey))
    )

  def fetch(
             traces: Set[Trace],
             joinType: JoinType = spooky.conf.defaultJoinType,
             flattenPagesPattern: String = ".*",
             flattenPagesOrdinalKey: Symbol = null,
             numPartitions: Int = spooky.conf.defaultParallelism(this),
             optimizer: QueryOptimizer = spooky.conf.defaultQueryOptimizer
             ): PageRowRDD = Operations.Fetch(traces, joinType, flattenPagesPattern, flattenPagesOrdinalKey, numPartitions, optimizer).apply(this)

  def visit(
             expr: Expression[Any],
             filter: DocumentFilter = Const.defaultDocumentFilter,
             failSafe: Int = -1,
             joinType: JoinType = spooky.conf.defaultJoinType,
             numPartitions: Int = spooky.conf.defaultParallelism(this),
             optimizer: QueryOptimizer = spooky.conf.defaultQueryOptimizer
             ): PageRowRDD = {

    var trace: Set[Trace] =  (
      Visit(expr)
        +> Snapshot(filter)
      )
    if (failSafe > 0) trace = Try(trace, failSafe)

    this.fetch(
      trace,
      joinType = joinType,
      numPartitions = numPartitions,
      optimizer = optimizer
    )
  }

  def wget(
            expr: Expression[Any],
            filter: DocumentFilter = Const.defaultDocumentFilter,
            failSafe: Int = -1,
            joinType: JoinType = spooky.conf.defaultJoinType,
            numPartitions: Int = spooky.conf.defaultParallelism(this),
            optimizer: QueryOptimizer = spooky.conf.defaultQueryOptimizer
            ): PageRowRDD = {

    var trace: Set[Trace] =  Wget(expr, filter)

    if (failSafe > 0) trace = Try(trace, failSafe)

    this.fetch(
      trace,
      joinType = joinType,
      numPartitions = numPartitions,
      optimizer = optimizer
    )
  }

  private[sparkbinding] def _narrowFetch(
                                          _traces: Set[Trace],
                                          joinType: JoinType,
                                          numPartitions: Int
                                          ): PageRowRDD = {

    val spooky = this.spooky

    val trace_RowRDD: RDD[(Trace, PageRow)] = selfRDD
      .flatMap {
        row =>
          _traces.interpolate(row, spooky).map(interpolatedTrace => interpolatedTrace -> row.clearPagesBeforeFetch(joinType))
      }
      .repartition(numPartitions)

    val resultRows = trace_RowRDD
      .flatMap {
        tuple =>
          val pages = tuple._1.fetch(spooky)

          tuple._2.putPages(pages, joinType)
      }

    this.copy(selfRDD = resultRows)
  }

  private[sparkbinding] def _wideFetch(
                                        _traces: Set[Trace],
                                        joinType: JoinType,
                                        numPartitions: Int,
                                        useWebCache: Boolean,
                                        postProcessing: PageRow => Iterable[PageRow] = Some(_)
                                        )(
                                        seed: Boolean = false,
                                        seedFilter: Iterable[PageRow] => Option[PageRow] = _ => None //by default nothing will be inserted into explored rows
                                        ): PageRowRDD = {

    val spooky = this.spooky

    val trace_Rows: RDD[(Trace, PageRow)] = selfRDD
      .flatMap {
        row =>
          _traces.interpolate(row, spooky).map(interpolatedTrace => interpolatedTrace -> row.clearPagesBeforeFetch(joinType))
      }

    val updated: PageRowRDD = if (!useWebCache) {
      val pageRows = trace_Rows
        .groupByKey(numPartitions)
        .flatMap {
          tuple =>
            val pages = tuple._1.fetch(spooky)
            val newRows = tuple._2.flatMap(_.putPages(pages, joinType)).flatMap(postProcessing)
            newRows
        }

      this.copy(selfRDD = pageRows)
    }
    else {
      val coGrouped: RDD[(DryRun, (Iterable[(Trace, PageRow)], Iterable[Squashed[PageRow]]))] =
        trace_Rows.keyBy(_._1.dryrun).cogroup(webCacheRDD, numPartitions)

      val newRows_newCache: RDD[(Iterable[PageRow], WebCacheRow)] = coGrouped.map {
        triplet =>
          val tuple = triplet._2
          val rows: Iterable[PageRow] = tuple._1.map(_._2)
          val cachedSquashedRowOption = tuple._2.reduceOption(_ ++ _)

          cachedSquashedRowOption match {
            case None => //didn't find anything, resolve is the only option, add all new pages into lookup
              val pageLikes = triplet._2._1.map(_._1).reduce {
                (s1, s2) =>
                  if (s1.length < s2.length) s1
                  else s2
              }.fetch(spooky)
              val fetchedRows = rows.flatMap(_.putPages(pageLikes, joinType))
              val seedRows = fetchedRows
                .groupBy(_.segmentID)
                .flatMap(tuple => seedFilter(tuple._2))
              val seedRowsPost = seedRows.flatMap(postProcessing)

              val newSelf = if (!seed) fetchedRows.flatMap(postProcessing)
              else seedRowsPost
              val newCached = triplet._1 -> Squashed(pageLikes.toArray, seedRowsPost.toArray)

              newSelf -> newCached
            case Some(cachedSquashedRow) => //found something, use same pages, add into rows except those found with existing segmentIDs, only add new pages with non-existing segmentIDs into lookup
              val dryrun = triplet._1
              val meta = cachedSquashedRow.rows
              val uniqueMeta = meta.groupBy(_.segmentID).values.flatMap(seedFilter(_))
              val uniqueCachedSquashedRow = cachedSquashedRow.copy(rows = uniqueMeta.toArray)

              uniqueCachedSquashedRow.pageLikes.foreach {
                pageLike =>
                  val sameBacktrace = dryrun.find(_ == pageLike.uid.backtrace).get
                  pageLike.uid.backtrace.injectFrom(sameBacktrace)
              }
              val pageLikes: Array[PageLike] = uniqueCachedSquashedRow.pageLikes
              val fetchedRows = rows.flatMap(_.putPages(pageLikes, joinType))

              val existingSegIDs = uniqueCachedSquashedRow.rows.map(_.segmentID).toSet
              val seedRows = fetchedRows.filterNot(row => existingSegIDs.contains(row.segmentID))
                .groupBy(_.segmentID)
                .flatMap(tuple => seedFilter(tuple._2))
              val seedRowsPost = seedRows.flatMap(postProcessing)

              val newSelf = if (!seed) fetchedRows.flatMap(postProcessing)
              else seedRowsPost
              val newCached = triplet._1 -> uniqueCachedSquashedRow.copy(rows = uniqueCachedSquashedRow.rows ++ seedRowsPost)

              newSelf -> newCached
          }
      }

      newRows_newCache.name = s"""
                                 |${Wide_RDDWebCache.getClass.getSimpleName.stripSuffix("$")} fetch
        """.stripMargin.trim

      this.persistTemp(newRows_newCache) //TODO: unpersist

      this.copy(selfRDD = newRows_newCache.flatMap(_._1), webCache = newRows_newCache.map(_._2))
    }

    updated
  }

  def join(
            expr: Expression[Any], //name is discarded
            ordinalKey: Symbol = null, //left & idempotent parameters are missing as they are always set to true
            maxOrdinal: Int = spooky.conf.maxJoinOrdinal,
            distinct: Boolean = false //experimental: set to true to eliminate duplicates in join key
            )(
            traces: Set[Trace],
            joinType: JoinType = spooky.conf.defaultJoinType,
            numPartitions: Int = spooky.conf.defaultParallelism(this),
            flattenPagesPattern: String = ".*",
            flattenPagesOrdinalKey: Symbol = null,
            optimizer: QueryOptimizer = spooky.conf.defaultQueryOptimizer
            )(
            select: Expression[Any]*
            ): PageRowRDD = Operations.Join(expr, ordinalKey, maxOrdinal, distinct)(
    traces, joinType, numPartitions, flattenPagesPattern, flattenPagesOrdinalKey, optimizer
  )(select: _*).apply(this)

  /**
   * results in a new set of Pages by crawling links on old pages
   * old pages that doesn't contain the link will be ignored
   * @param maxOrdinal only the first n links will be used, default to Const.fetchLimit
   * @return RDD[Page]
   */
  def visitJoin(
                 expr: Expression[Any],
                 filter: DocumentFilter = Const.defaultDocumentFilter,
                 failSafe: Int = -1,
                 ordinalKey: Symbol = null, //left & idempotent parameters are missing as they are always set to true
                 maxOrdinal: Int = spooky.conf.maxJoinOrdinal,
                 distinct: Boolean = false, //set to true to eliminate duplates in join key
                 joinType: JoinType = spooky.conf.defaultJoinType,
                 numPartitions: Int = spooky.conf.defaultParallelism(this),
                 select: Expression[Any] = null,
                 selects: Traversable[Expression[Any]] = Seq(),
                 optimizer: QueryOptimizer = spooky.conf.defaultQueryOptimizer
                 ): PageRowRDD ={

    var trace: Set[Trace] =  (
      Visit(new GetExpr(Const.defaultJoinKey))
        +> Snapshot(filter)
      )
    if (failSafe > 0) trace = Try(trace, failSafe)

    this.join(expr, ordinalKey, maxOrdinal, distinct)(
      trace,
      joinType,
      numPartitions,
      optimizer = optimizer
    )(Option(select).toSeq ++ selects: _*)
  }


  /**
   * same as join, but avoid launching a browser by using direct http GET (wget) to download new pages
   * much faster and less stressful to both crawling and target server(s)
   * @param maxOrdinal only the first n links will be used, default to Const.fetchLimit
   * @return RDD[Page]
   */
  def wgetJoin(
                expr: Expression[Any],
                filter: DocumentFilter = Const.defaultDocumentFilter,
                failSafe: Int = -1,
                ordinalKey: Symbol = null, //left & idempotent parameters are missing as they are always set to true
                maxOrdinal: Int = spooky.conf.maxJoinOrdinal,
                distinct: Boolean = false, //set to true to eliminate duplates in join key
                joinType: JoinType = spooky.conf.defaultJoinType,
                numPartitions: Int = spooky.conf.defaultParallelism(this),
                select: Expression[Any] = null,
                selects: Traversable[Expression[Any]] = Seq(),
                optimizer: QueryOptimizer = spooky.conf.defaultQueryOptimizer
                ): PageRowRDD ={

    var trace: Set[Trace] =  Wget(new GetExpr(Const.defaultJoinKey), filter)
    if (failSafe > 0) trace = Try(trace, failSafe)

    this.join(expr, ordinalKey, maxOrdinal, distinct)(
      trace,
      joinType,
      numPartitions,
      optimizer = optimizer
    )(Option(select).toSeq ++ selects: _*)
  }

  def explore(
               expr: Expression[Any],
               depthKey: Symbol = null,
               maxDepth: Int = spooky.conf.maxExploreDepth,
               ordinalKey: Symbol = null,
               maxOrdinal: Int = spooky.conf.maxJoinOrdinal,
               checkpointInterval: Int = spooky.conf.checkpointInterval
               )(
               traces: Set[Trace],
               numPartitions: Int = spooky.conf.defaultParallelism(this),
               flattenPagesPattern: String = ".*",
               flattenPagesOrdinalKey: Symbol = null,
               optimizer: QueryOptimizer = spooky.conf.defaultQueryOptimizer
               )(
               select: Expression[Any]*
               ): PageRowRDD =
    Operations.Explore(expr, depthKey, maxDepth, ordinalKey, maxOrdinal, checkpointInterval)(
      traces, numPartitions, flattenPagesPattern, flattenPagesOrdinalKey, optimizer
    )(select: _*).apply(this)

  //this is a single-threaded explore, of which implementation is similar to good old pagination.
  //may fetch same page twice or more if pages of this can reach each others. TODO: Deduplicate happens between stages
  private[sparkbinding] def _narrowExplore(
                                            expr: Expression[Any],
                                            depthKey: Symbol,
                                            maxDepth: Int,
                                            ordinalKey: Symbol,
                                            maxOrdinal: Int,
                                            checkpointInterval: Int
                                            )(
                                            _traces: Set[Trace],
                                            numPartitions: Int,
                                            flattenPagesPattern: String,
                                            flattenPagesOrdinalKey: Symbol
                                            )(
                                            select: Expression[Any]*
                                            ): PageRowRDD = {

    val spooky = this.spooky
    if (this.context.getCheckpointDir.isEmpty) this.context.setCheckpointDir(spooky.conf.dirs.checkpoint)

    val _expr = expr defaultAs Symbol(Const.defaultJoinKey)

    var depthFromExclusive = 0

    val firstResultRDD = this
      .select(Option(depthKey).map(key => Literal(0) ~ key).toSeq: _*)

    firstResultRDD.name =
      """
        |Narrow explore
        |Depth: 0
      """.stripMargin.trim

    firstResultRDD.persistTemp(firstResultRDD.selfRDD)
    val firstCount = firstResultRDD.count()

    if (firstCount == 0) return this.copy(selfRDD = sparkContext.emptyRDD)

    val firstStageRDD = firstResultRDD
      .map {
        row =>
          val dryrun: DryRun = row.pageLikes.toList.map(_.uid.backtrace).distinct
          val seeds = Seq(row)
          val preJoins = seeds.flatMap{
            _.localPreJoins(_expr,ordinalKey,maxOrdinal, spooky)(
              _traces, existingDryruns = Set(dryrun)
            )
          }

          ExploreStage(preJoins.toArray, existingDryruns = Set(dryrun))
      }

    val resultRDDs = ArrayBuffer[RDD[PageRow]](
      firstResultRDD
    )

    val resultKeys = this.keys ++
      Seq(
        TempKey(_expr.name),
        Key.depthKey(depthKey, maxDepth),
        Key.ordinalKey(ordinalKey),
        Key.ordinalKey(flattenPagesOrdinalKey)).flatMap(Option(_)
      )

    var stageRDD = firstStageRDD.repartition(numPartitions)
    while(true) {

      val _depthFromExclusive = depthFromExclusive //var in closure being shipped to workers usually end up miserably (not synched properly)
      val depthToInclusive = Math.min(_depthFromExclusive + Const.exploreStageSize, maxDepth)

      val newRows_newStageRDD: RDD[(Iterable[PageRow], ExploreStage)] = stageRDD.map {
        stage =>
          PageRow.localExplore(
            stage,
            spooky
          )(
            _expr,
            depthKey,
            _depthFromExclusive,
            depthToInclusive,
            ordinalKey,
            maxOrdinal
          )(
            _traces,
            flattenPagesPattern,
            flattenPagesOrdinalKey
          )
      }

      newRows_newStageRDD.name =
        s"""
           |Narrow explore
           |Depth: ${_depthFromExclusive} to $depthToInclusive
        """.stripMargin.trim

      this.persistTemp(newRows_newStageRDD)

      val newRows = newRows_newStageRDD.flatMap(_._1)
      resultRDDs += newRows

      stageRDD = newRows_newStageRDD.map(_._2)
        .filter(_.hasMore)
        .repartition(numPartitions)

      val count = stageRDD.count()
      LoggerFactory.getLogger(this.getClass).info(s"$count segment(s) have uncrawled seed(s) after $depthToInclusive iteration(s)")
      depthFromExclusive = depthToInclusive

      if (count == 0) {
        LoggerFactory.getLogger(this.getClass).info("Narrow explore completed: all documents has been crawled")
        return result
      }
      else if (depthToInclusive >= maxDepth) {
        LoggerFactory.getLogger(this.getClass).info(s"Narrow explore completed: reached maximum number of iterations ($maxDepth)")
        return result
      }
    }

    def result: PageRowRDD = {

      val resultSelf = new UnionRDD(sparkContext, resultRDDs).coalesce(numPartitions)
      val result = this.copy(selfRDD = resultSelf, keys = resultKeys).select(select: _*)

      result
    }

    throw new RuntimeException("INTERNAL ERROR: unreachable")
  }

  //recursive join and union! applicable to many situations like (wide) pagination and deep crawling
  private[sparkbinding] def _wideExplore(
                                          expr: Expression[Any],
                                          depthKey: Symbol,
                                          maxDepth: Int,
                                          ordinalKey: Symbol,
                                          maxOrdinal: Int,
                                          checkpointInterval: Int,
                                          useWebCache: Boolean
                                          )(
                                          _traces: Set[Trace],
                                          numPartitions: Int,
                                          flattenPagesPattern: String,
                                          flattenPagesOrdinalKey: Symbol
                                          )(
                                          select: Expression[Any]*
                                          ): PageRowRDD = {

    val spooky = this.spooky
    if (this.context.getCheckpointDir.isEmpty) this.context.setCheckpointDir(spooky.conf.dirs.checkpoint)

    val _expr = expr defaultAs Symbol(Const.defaultJoinKey)

    val depth0 = this.select(Option(depthKey).map(key => Literal(0) ~ key).toSeq: _*)
    this.persistTemp(depth0.selfRDD) //TODO: clean it up?

    val partitioner = new HashPartitioner(spooky.conf.defaultParallelism.apply(selfRDD))

    val WebCache0 = if (useWebCache) {
      depth0.webCacheRDD.putRows(depth0.selfRDD, partitionerOption = Some(partitioner))
    }
    else {
      depth0.selfRDDToWebCacheRDD(partitionerOption = Some(partitioner))
    }
    var seeds: PageRowRDD = depth0
      .copy(webCache = WebCache0)

    seeds.name =
      """
        |Wide explore
        |Depth: 0
      """.stripMargin.trim

    val flatten: PageRow => Iterable[PageRow] = {
      row =>
        row.flattenPages(flattenPagesPattern, flattenPagesOrdinalKey)
    }

    val seedFilter: Iterable[PageRow] => Option[PageRow] = rows => rows.reduceOption(PageRow.reducer(ordinalKey))

    for (depth <- 1 to maxDepth) {
      val newRows: PageRowRDD = seeds
        .select_!(Option(depthKey).map(key => Literal(depth) ~ key).toSeq: _*)
        .flattenTemp(_expr, ordinalKey, maxOrdinal, left = true)
        ._wideFetch(_traces, Inner, numPartitions, useWebCache = true,
          postProcessing = flatten
        )(
          seed = true,
          seedFilter
        )

      val newRowsCount = newRows.count()
      LoggerFactory.getLogger(this.getClass).info(s"found $newRowsCount new row(s) after $depth iterations")

      if (newRowsCount == 0) return result

      seeds = newRows
      seeds.name =
        s"""
           |Wide explore
           |Depth: $depth
        """.stripMargin.trim
    }

    def result = {
      val resultSelf = seeds.webCacheRDD.getRows
      val resultWebCache = if (useWebCache) seeds.webCacheRDD.discardDataRows
      else this.webCacheRDD
      val resultKeys = this.keys ++
        Seq(
          TempKey(_expr.name),
          Key.depthKey(depthKey, maxDepth),
          Key.ordinalKey(ordinalKey),
          Key.ordinalKey(flattenPagesOrdinalKey)).flatMap(Option(_)
        )

      this.copy(resultSelf, resultWebCache, resultKeys)
        .select(select: _*)
    }

    result
  }

  def visitExplore(
                    expr: Expression[Any],
                    filter: DocumentFilter = Const.defaultDocumentFilter,
                    failSafe: Int = -1,
                    depthKey: Symbol = null,
                    maxDepth: Int = spooky.conf.maxExploreDepth,
                    ordinalKey: Symbol = null,
                    maxOrdinal: Int = spooky.conf.maxJoinOrdinal,
                    checkpointInterval: Int = spooky.conf.checkpointInterval,
                    numPartitions: Int = spooky.conf.defaultParallelism(this),
                    select: Expression[Any] = null,
                    selects: Traversable[Expression[Any]] = Seq(),
                    optimizer: QueryOptimizer = spooky.conf.defaultQueryOptimizer
                    ): PageRowRDD = {

    var trace: Set[Trace] =  (
      Visit(new GetExpr(Const.defaultJoinKey))
        +> Snapshot(filter)
      )
    if (failSafe > 0) trace = Try(trace, failSafe)

    explore(expr, depthKey, maxDepth, ordinalKey, maxOrdinal, checkpointInterval)(
      trace,
      numPartitions,
      optimizer = optimizer
    )(Option(select).toSeq ++ selects: _*)
  }

  def wgetExplore(
                   expr: Expression[Any],
                   filter: DocumentFilter = Const.defaultDocumentFilter,
                   failSafe: Int = -1,
                   depthKey: Symbol = null,
                   maxDepth: Int = spooky.conf.maxExploreDepth,
                   ordinalKey: Symbol = null,
                   maxOrdinal: Int = spooky.conf.maxJoinOrdinal,
                   checkpointInterval: Int = spooky.conf.checkpointInterval,
                   numPartitions: Int = spooky.conf.defaultParallelism(this),
                   select: Expression[Any] = null,
                   selects: Traversable[Expression[Any]] = Seq(),
                   optimizer: QueryOptimizer = spooky.conf.defaultQueryOptimizer
                   ): PageRowRDD = {

    var trace: Set[Trace] =  Wget(new GetExpr(Const.defaultJoinKey), filter)
    if (failSafe > 0) trace = Try(trace, failSafe)

    explore(expr, depthKey, maxDepth, ordinalKey, maxOrdinal, checkpointInterval)(
      trace,
      numPartitions,
      optimizer = optimizer
    )(Option(select).toSeq ++ selects: _*)
  }
}