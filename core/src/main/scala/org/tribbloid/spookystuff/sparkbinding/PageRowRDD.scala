package org.tribbloid.spookystuff.sparkbinding

import org.slf4j.LoggerFactory
import org.apache.spark.HashPartitioner
import org.apache.spark.rdd.{RDD, UnionRDD}
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.storage.StorageLevel
import org.tribbloid.spookystuff.actions._
import org.tribbloid.spookystuff.dsl._
import org.tribbloid.spookystuff.entity.PageRow.{WebCacheRow, WebCacheRDD}
import org.tribbloid.spookystuff.entity._
import org.tribbloid.spookystuff.expressions._
import org.tribbloid.spookystuff.pages.{PageLike, Unstructured}
import org.tribbloid.spookystuff.utils._
import org.tribbloid.spookystuff.{Const, QueryException, SpookyContext, views}

import scala.collection.immutable.ListSet
import scala.collection.mutable.ArrayBuffer
import scala.language.implicitConversions
import scala.util.Random

/**
 * Created by peng on 8/29/14.
 * Core component, abstraction of distributed Page + schemaless KVStore to represent all stages of remote resource discovery
 */
class PageRowRDD private (
                           val self: RDD[PageRow],
                           val webCache: WebCacheRDD, //in Memory cache, used for both quick page lookup from memory and seeding
                           val keys: ListSet[KeyLike],
                           val spooky: SpookyContext,
                           val persisted: ArrayBuffer[RDD[_]]
                           )
  extends PageRowRDDApi {

  import RDD._
  import views._

  def this(
            self: RDD[PageRow],
            spooky: SpookyContext
            ) = this(

    self,
    self.sparkContext.emptyRDD[WebCacheRow].partitionBy(new HashPartitioner(spooky.conf.defaultParallelism(self))),
    ListSet(), spooky, ArrayBuffer())

  def this(
            self: RDD[PageRow],
            keys: ListSet[KeyLike],
            spooky: SpookyContext
            ) = this(

    self,
    self.sparkContext.emptyRDD[WebCacheRow].partitionBy(new HashPartitioner(spooky.conf.defaultParallelism(self))),
    keys, spooky, ArrayBuffer())

  def copy(
            self: RDD[PageRow] = this.self,
            webCache: WebCacheRDD = this.webCache,
            keys: ListSet[KeyLike] = this.keys,
            spooky: SpookyContext = this.spooky,
            persisted: ArrayBuffer[RDD[_]] = this.persisted
            ): PageRowRDD = {

    val result = new PageRowRDD(
      self,
      webCache,
      keys, spooky, persisted
    )
    result
  }

  def segmentBy(exprs: (PageRow => Any)*): PageRowRDD = {

    this.copy(self.map{
      row =>
        val values = exprs.map(_.apply(row))
        row.copy(segmentID = values.hashCode())
    })
  }

  def segmentByRow: PageRowRDD = {
    this.copy(self.map(_.copy(segmentID = Random.nextLong())))
  }

  private def discardPages: PageRowRDD = this.copy(self = self.map(_.copy(pageLikes = Array())))

  //  private def discardWebCache: PageRowRDD = {
  //    this.webCache.unpersist(blocking = false)
  //    this.copy(webCache = sparkContext.emptyRDD[WebCacheRow].partitionBy(new HashPartitioner(spooky.conf.defaultParallelism(self))))
  //  }

  private def discardExploredRows: PageRowRDD = {
    this.copy(webCache = webCache.discardRows)
  }

  def unpersistAllRDDs(): PageRowRDD = {
    this.persisted.foreach(_.unpersist(blocking = false))
    this.persisted.clear()
    this
  }

  @transient def keysSeq: Seq[KeyLike] = this.keys.toSeq.reverse

  @transient def sortKeysSeq: Seq[SortKey] = keysSeq.flatMap{
    case k: SortKey => Some(k)
    case _ => None
  }

  private def defaultSort: PageRowRDD = {

    val sortKeysSeq = this.sortKeysSeq

    import scala.Ordering.Implicits._

    self.name = "sort"
    if (this.getStorageLevel == StorageLevel.NONE){
      this.persisted += self.persist(spooky.conf.defaultStorageLevel)
    }

    val result = this.sortBy{_.ordinal(sortKeysSeq)}
    result.foreachPartition{_ =>}
    this.unpersistAllRDDs()
    result
  }

  def toMapRDD(sort: Boolean = false): RDD[Map[String, Any]] =
    if (!sort) this.map(_.toMap)
    else this
      .discardPages
      .defaultSort
      .map(_.toMap)

  def toJSON(sort: Boolean = false): RDD[String] =
    if (!sort) this.map(_.toJSON)
    else this
      .discardPages
      .defaultSort
      .map(_.toJSON)

  //TODO: investigate using the new applySchema api to avoid losing type info
  def toDF(sort: Boolean = false, tableName: String = null): DataFrame = {

    val jsonRDD = this.toJSON(sort)

    val schemaRDD = this.spooky.sqlContext.jsonRDD(jsonRDD)

    val validKeyNames = keysSeq
      .filter(key => key.isInstanceOf[Key])
      .map(key => Utils.canonizeColumnName(key.name))
      .filter(name => schemaRDD.schema.fieldNames.contains(name))
    val columns = validKeyNames.map(name => new Column(UnresolvedAttribute(name)))

    val result = schemaRDD.select(columns: _*)

    if (tableName!=null) result.registerTempTable(tableName)

    result
  }

  def toCSV(separator: String = ","): RDD[String] = this.toDF().map {
    _.mkString(separator)
  }

  def toTSV: RDD[String] = this.toCSV("\t")

  /**
   * save each page to a designated directory
   * this is a narrow transformation, use it to save overhead for scheduling
   * support many file systems including but not limited to HDFS, S3 and local HDD
   * @param overwrite if a file with the same name already exist:
   *                  true: overwrite it
   *                  false: append an unique suffix to the new file name
   * @return the same RDD[Page] with file paths carried as metadata
   */
  //always use the same path pattern for filtered pages, if you want pages to be saved with different path, use multiple saveContent with different names
  def savePages(
                 path: Expression[Any],
                 name: Symbol = null,
                 overwrite: Boolean = false
                 ): PageRowRDD = {

    this.spooky.broadcast()
    val spooky = this.spooky

    val saved = this.map {

      pageRow =>
        val pathStr = path(pageRow)

        pathStr.foreach {
          str =>
            val strCanon = str
            val page =
              if (name == null || name.name == Const.onlyPageWildcard) pageRow.getOnlyPage
              else pageRow.getPage(name.name)

            page.foreach(_.save(Seq(strCanon.toString), overwrite)(spooky))
        }
        pageRow
    }
    this.copy(self = saved)
  }

  /**
   * same as saveAs
   * but this is an action that will be executed immediately
   * @param overwrite if a file with the same name already exist:
   *                  true: overwrite it
   *                  false: append an unique suffix to the new file name
   * @return an array of file paths
   */
  def dumpPages(
                 path: Expression[String],
                 name: Symbol = null,
                 overwrite: Boolean = false
                 ): Array[ListSet[String]] = this.savePages(path, name, overwrite).flatMap {
    _.pages.map {
      _.saved
    }
  }.collect()

  //  /**
  //   * extract parts of each Page and insert into their respective context
  //   * if a key already exist in old context it will be replaced with the new one.
  //   * @param exprs
  //   * @return new PageRowRDD
  //   */
  def select(exprs: Expression[Any]*): PageRowRDD = {

    val newKeys: Seq[Key] = exprs.map {
      expr =>
        val key = Key(expr.name)
        if(this.keys.contains(key) && !expr.isInstanceOf[InsertIntoExpr[_]]) //can't insert the same key twice
          throw new QueryException(s"Key ${key.name} already exist")
        key
    }

    val result = this.copy(
      self = this.flatMap(_.select(exprs: _*)),
      keys = this.keys ++ newKeys
    )
    result
  }

  //no "already exist" check
  def selectOverwrite(exprs: Expression[Any]*): PageRowRDD = {

    val newKeys: Seq[Key] = exprs.map {
      expr => Key(expr.name)
    }

    val result = this.copy(
      self = this.flatMap(_.select(exprs: _*)),
      keys = this.keys ++ newKeys
    )
    result
  }

  private def selectTemp(exprs: Expression[Any]*): PageRowRDD = {

    val newKeys: Seq[TempKey] = exprs.map {
      expr =>
        val key = TempKey(expr.name)
        key
    }

    this.copy(
      self = this.flatMap(_.selectTemp(exprs: _*)),
      keys = this.keys ++ newKeys
    )
  }

  def remove(keys: Symbol*): PageRowRDD = {
    val names = keys.map(key => Key(key))
    this.copy(
      self = this.map(_.remove(names: _*)),
      keys = this.keys -- names
    )
  }

  private def clearTemp: PageRowRDD = {
    this.copy(
      self = this.map(_.clearTemp),
      keys = keys -- keys.filter(_.isInstanceOf[TempKey])//circumvent https://issues.scala-lang.org/browse/SI-8985
    )
  }

  def flatten(
               expr: Expression[Any],
               ordinalKey: Symbol = null,
               maxOrdinal: Int = Int.MaxValue,
               left: Boolean = true
               ): PageRowRDD = {
    val selected = this.select(expr)

    val flattened = selected.flatMap(_.flatten(expr.name, ordinalKey, maxOrdinal, left))
    selected.copy(
      self = flattened,
      keys = selected.keys ++ Option(Key.sortKey(ordinalKey))
    )
  }

  private def flattenTemp(
                           expr: Expression[Any],
                           ordinalKey: Symbol = null,
                           maxOrdinal: Int = Int.MaxValue,
                           left: Boolean = true
                           ): PageRowRDD = {
    val selected = this.selectTemp(expr)

    val flattened = selected.flatMap(_.flatten(expr.name, ordinalKey, maxOrdinal, left))
    selected.copy(
      self = flattened,
      keys = selected.keys ++ Option(Key.sortKey(ordinalKey))
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
                  expr: Expression[Seq[Unstructured]], //avoid confusion
                  ordinalKey: Symbol = null,
                  maxOrdinal: Int = Int.MaxValue,
                  left: Boolean = true
                  )(exprs: Expression[Any]*) ={

    this
      .flattenTemp(expr defaultAs Symbol(Const.defaultJoinKey), ordinalKey, maxOrdinal, left)
      .select(exprs: _*)
  }

  def flattenPages(
                    pattern: String = ".*", //TODO: enable it
                    ordinalKey: Symbol = null
                    ): PageRowRDD =

    this.copy(
      self = this.flatMap(_.flattenPages(pattern.name, ordinalKey)),
      keys = this.keys ++ Option(Key.sortKey(ordinalKey))
    )

  def fetch(
             traces: Set[Trace],
             joinType: JoinType = Const.defaultJoinType,
             flattenPagesPattern: String = ".*",
             flattenPagesOrdinalKey: Symbol = null,
             numPartitions: Int = spooky.conf.defaultParallelism(this),
             optimizer: QueryOptimizer = spooky.conf.defaultQueryOptimizer
             ): PageRowRDD = {

    val _traces = traces.autoSnapshot

    spooky.broadcast()

    val result = optimizer match {
      case Narrow =>
        _narrowFetch(_traces, joinType, numPartitions)
      case Wide =>
        _wideFetch(_traces, joinType, numPartitions, useWebCache = false)()
          .discardExploredRows //optional
      case Wide_WebCachedRDD =>
        _wideFetch(_traces, joinType, numPartitions, useWebCache = true)()
          .discardExploredRows
      case _ => throw new UnsupportedOperationException(s"${optimizer.getClass.getSimpleName} optimizer is not supported in this query")
    }

    if (flattenPagesPattern != null) result.flattenPages(flattenPagesPattern,flattenPagesOrdinalKey)
    else result
  }

  private def _narrowFetch(
                            _traces: Set[Trace],
                            joinType: JoinType,
                            numPartitions: Int
                            ): PageRowRDD = {

    val spooky = this.spooky

    val trace_RowRDD: RDD[(Trace, PageRow)] = self
      .flatMap {
      row =>
        _traces.interpolate(row).map(interpolatedTrace => interpolatedTrace -> row.clearPagesBeforeFetch(joinType))
    }
      .repartition(numPartitions)

    val resultRows = trace_RowRDD
      .flatMap {
      tuple =>
        val pages = tuple._1.resolve(spooky)

        tuple._2.putPages(pages, joinType)
    }

    this.copy(self = resultRows)
  }

  private def _wideFetch(
                          _traces: Set[Trace],
                          joinType: JoinType,
                          numPartitions: Int,
                          useWebCache: Boolean,
                          postProcessing: PageRow => Iterable[PageRow] = Some(_)
                          )(
                          seed: Boolean = false,
                          seedFilter: Iterable[PageRow] => Option[PageRow] = _=>None //by default nothing will be inserted into explored rows
                          ): PageRowRDD = {

    val spooky = this.spooky

    val trace_Rows: RDD[(Trace, PageRow)] = self
      .flatMap {
      row =>
        _traces.interpolate(row).map(interpolatedTrace => interpolatedTrace -> row.clearPagesBeforeFetch(joinType))
    }

    val updated: PageRowRDD = if (!useWebCache) {
      val pageRows = trace_Rows
        .groupByKey(numPartitions)
        .flatMap {
        tuple =>
          val pages = tuple._1.resolve(spooky)
          val newRows = tuple._2.flatMap(_.putPages(pages, joinType)).flatMap(postProcessing)
          newRows
      }

      this.copy(self = pageRows)
    }
    else {
      val cogrouped: RDD[(DryRun, (Iterable[(Trace, PageRow)], Iterable[Squashed[PageRow]]))] =
        trace_Rows.keyBy(_._1.dryrun).cogroup(webCache, numPartitions)

      val newRows_newCache = cogrouped.map {
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
              }.resolve(spooky)
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
              val meta = cachedSquashedRow.metadata
              val uniqueMeta = meta.groupBy(_.segmentID).values.flatMap(seedFilter(_))
              val uniqueCachedSquashedRow = cachedSquashedRow.copy(metadata = uniqueMeta.toArray)

              uniqueCachedSquashedRow.pageLikes.foreach {
                pageLike =>
                  val sameBacktrace = dryrun.find(_ == pageLike.uid.backtrace).get
                  pageLike.uid.backtrace.injectFrom(sameBacktrace)
              }
              val pageLikes: Array[PageLike] = uniqueCachedSquashedRow.pageLikes
              val fetchedRows = rows.flatMap(_.putPages(pageLikes, joinType))

              val existingSegIDs = uniqueCachedSquashedRow.metadata.map(_.segmentID).toSet
              val seedRows = fetchedRows.filterNot(row => existingSegIDs.contains(row.segmentID))
                .groupBy(_.segmentID)
                .flatMap(tuple => seedFilter(tuple._2))
              val seedRowsPost = seedRows.flatMap(postProcessing)

              val newSelf = if (!seed) fetchedRows.flatMap(postProcessing)
              else seedRowsPost
              val newCached = triplet._1 -> uniqueCachedSquashedRow.copy(metadata = uniqueCachedSquashedRow.metadata ++ seedRowsPost)

              newSelf -> newCached
          }
      }

      newRows_newCache.name = s"""
                                 |fetch (optimizer=${Wide_WebCachedRDD.getClass.getSimpleName})
        """.stripMargin.trim
      newRows_newCache.persist()

      this.copy(self = newRows_newCache.flatMap(_._1), webCache = newRows_newCache.map(_._2))
    }

    updated
  }

  def join(
            expr: Expression[Any], //name is discarded
            ordinalKey: Symbol = null, //left & idempotent parameters are missing as they are always set to true
            maxOrdinal: Int = spooky.conf.maxJoinOrdinal
            )(
            traces: Set[Trace],
            joinType: JoinType = Const.defaultJoinType,
            numPartitions: Int = spooky.conf.defaultParallelism(this),
            flattenPagesPattern: String = ".*",
            flattenPagesOrdinalKey: Symbol = null,
            optimizer: QueryOptimizer = spooky.conf.defaultQueryOptimizer
            )(
            select: Expression[Any]*
            ): PageRowRDD = {

    this
      .flattenTemp(expr defaultAs Symbol(Const.defaultJoinKey), ordinalKey, maxOrdinal, left = true)
      .fetch(traces, joinType, flattenPagesPattern, flattenPagesOrdinalKey, numPartitions, optimizer)
      .select(select: _*)
  }

  /**
   * results in a new set of Pages by crawling links on old pages
   * old pages that doesn't contain the link will be ignored
   * @param maxOrdinal only the first n links will be used, default to Const.fetchLimit
   * @return RDD[Page]
   */
  def visitJoin(
                 expr: Expression[Any],
                 hasTitle: Boolean = true,
                 ordinalKey: Symbol = null, //left & idempotent parameters are missing as they are always set to true
                 maxOrdinal: Int = spooky.conf.maxJoinOrdinal,
                 joinType: JoinType = Const.defaultJoinType,
                 numPartitions: Int = spooky.conf.defaultParallelism(this),
                 select: Expression[Any] = null,
                 optimizer: QueryOptimizer = spooky.conf.defaultQueryOptimizer
                 ): PageRowRDD =
    this.join(expr, ordinalKey, maxOrdinal)(
      Visit(new GetExpr(Const.defaultJoinKey), hasTitle),
      joinType,
      numPartitions,
      optimizer = optimizer
    )(Option(select).toSeq: _*)

  /**
   * same as join, but avoid launching a browser by using direct http GET (wget) to download new pages
   * much faster and less stressful to both crawling and target server(s)
   * @param maxOrdinal only the first n links will be used, default to Const.fetchLimit
   * @return RDD[Page]
   */
  def wgetJoin(
                expr: Expression[Any],
                hasTitle: Boolean = true,
                ordinalKey: Symbol = null, //left & idempotent parameters are missing as they are always set to true
                maxOrdinal: Int = spooky.conf.maxJoinOrdinal,
                joinType: JoinType = Const.defaultJoinType,
                numPartitions: Int = spooky.conf.defaultParallelism(this),
                select: Expression[Any] = null,
                optimizer: QueryOptimizer = spooky.conf.defaultQueryOptimizer
                ): PageRowRDD =
    this.join(expr, ordinalKey, maxOrdinal)(
      Wget(new GetExpr(Const.defaultJoinKey), hasTitle),
      joinType,
      numPartitions,
      optimizer = optimizer
    )(Option(select).toSeq: _*)

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
               ): PageRowRDD = {

    val _traces = traces.autoSnapshot

    spooky.broadcast()

    val result = optimizer match {
      case Narrow =>
        _narrowExplore(expr, depthKey, maxDepth, ordinalKey, maxOrdinal, checkpointInterval)(_traces, numPartitions, flattenPagesPattern, flattenPagesOrdinalKey)(select: _*)
      case Wide =>
        _wideExplore(expr, depthKey, maxDepth, ordinalKey, maxOrdinal, checkpointInterval, useWebCache = false)(_traces, numPartitions, flattenPagesPattern, flattenPagesOrdinalKey)(select: _*)
      case Wide_WebCachedRDD =>
        _wideExplore(expr, depthKey, maxDepth, ordinalKey, maxOrdinal, checkpointInterval, useWebCache = true)(_traces, numPartitions, flattenPagesPattern, flattenPagesOrdinalKey)(select: _*)
      case _ => throw new UnsupportedOperationException(s"${optimizer.getClass.getSimpleName} optimizer is not supported in this query")
    }

    result
  }

  //this is a single-threaded explore, of which implementation is similar to good old pagination.
  //may fetch same page twice or more if pages of this can reach each others. TODO: Deduplicate happens between stages
  private def _narrowExplore(
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
      .clearTemp
      .select(Option(depthKey).map(key => Literal(0) ~ key).toSeq: _*)

    firstResultRDD.name =
      """
        |explore (optimizer=Narrow)
        |Depth: 0
      """.stripMargin.trim
    val firstCount = firstResultRDD.persistDuring(spooky.conf.defaultStorageLevel, blocking = false){
      firstResultRDD.checkpoint()
      firstResultRDD.count()
    }

    if (firstCount == 0) return this.copy(self = self.sparkContext.emptyRDD)

    val firstStageRDD = firstResultRDD
      .map {
      row =>
        val dryrun: DryRun = row.pageLikes.toSeq.map(_.uid.backtrace).distinct
        val seeds = Seq(row)
        val preJoins = seeds.flatMap{
          _.localPreJoins(_expr,ordinalKey,maxOrdinal)(
            _traces, existingDryruns = Set(dryrun)
          )
        }

        ExploreStage(preJoins.toArray, existingDryruns = Set(dryrun))
    }

    val resultRDDs = ArrayBuffer[RDD[PageRow]](
      firstResultRDD
    )

    val resultKeys = this.keys ++ Seq(TempKey(_expr.name), Key.sortKey(depthKey), Key.sortKey(ordinalKey), Key.sortKey(flattenPagesOrdinalKey)).flatMap(Option(_))

    var stageRDD = firstStageRDD.repartition(numPartitions)
    while(true) {

      val _depthFromExclusive = depthFromExclusive //var in closure being shipped to workers usually end up miserably (not synched properly)
      val depthToInclusive = Math.min(_depthFromExclusive + checkpointInterval, maxDepth)

      val newRows_newStageRDD = stageRDD.map {
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
           |explore (optimizer=Narrow)
           |Depth: ${_depthFromExclusive} to $depthToInclusive
        """.stripMargin.trim
      val count = newRows_newStageRDD.persistDuring(spooky.conf.defaultStorageLevel, blocking = false) {
        newRows_newStageRDD.checkpoint()

        val newRows = newRows_newStageRDD.flatMap(_._1)
        resultRDDs += newRows

        stageRDD = newRows_newStageRDD.map(_._2)
          .filter(_.hasMore)
          .repartition(numPartitions)

        stageRDD.count()
      }
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

      val resultSelf = new UnionRDD(this.sparkContext, resultRDDs).coalesce(numPartitions)
      val result = this.copy(self = resultSelf, keys = resultKeys).select(select: _*)
      result
    }

    throw new RuntimeException("unreachable")
  }

  //recursive join and union! applicable to many situations like (wide) pagination and deep crawling
  private def _wideExplore(
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

    self.name = "initializing wide explore"
    if (this.getStorageLevel == StorageLevel.NONE) {
      this.persisted += self.persist(spooky.conf.defaultStorageLevel)
    }
    val _expr = expr defaultAs Symbol(Const.defaultJoinKey)

    val depth0 = this.clearTemp.select(Option(depthKey).map(key => Literal(0) ~ key).toSeq: _*)

    val WebCache0 = {
      val webCache: WebCacheRDD = if (useWebCache) depth0.webCache
      else this.webCache.sparkContext.emptyRDD[WebCacheRow].partitionBy(new HashPartitioner(spooky.conf.defaultParallelism(self)))

      webCache.putRows(
        depth0.self,
        rows => rows.headOption
      )
    }
    var seeds = depth0
      .copy(webCache = WebCache0)

    val postProcessing: PageRow => Iterable[PageRow] = {
      row =>
        row.flattenPages(flattenPagesPattern, flattenPagesOrdinalKey)
    }

    val seedFilter: Iterable[PageRow] => Option[PageRow] = rows => rows.reduceOption(PageRow.reducer(ordinalKey))

    for (depth <- 1 to maxDepth) {
      val newRows = seeds
        .selectOverwrite(Option(depthKey).map(key => Literal(depth) ~ key).toSeq: _*)
        .flattenTemp(_expr, ordinalKey, maxOrdinal, left = true)
        ._wideFetch(_traces, Inner, numPartitions, useWebCache = true,
          postProcessing = postProcessing
        )(
          seed = true,
          seedFilter
        )

      val newRowsCount = newRows.count()
      LoggerFactory.getLogger(this.getClass).info(s"found $newRowsCount new row(s) after $depth iterations")

      if (newRowsCount == 0) return result

      seeds = newRows
    }

    def result = {
      val resultSelf = seeds.webCache.getRows
      val resultWebCache = if (useWebCache) seeds.webCache.discardRows
      else this.webCache
      val resultKeys = this.keys ++ Seq(TempKey(_expr.name), Key.sortKey(depthKey), Key.sortKey(ordinalKey), Key.sortKey(flattenPagesOrdinalKey)).flatMap(Option(_))

      this.copy(resultSelf, resultWebCache, resultKeys)
        .select(select: _*)
    }

    result
  }

  def visitExplore(
                    expr: Expression[Any],
                    hasTitle: Boolean = true,
                    depthKey: Symbol = null,
                    maxDepth: Int = spooky.conf.maxExploreDepth,
                    ordinalKey: Symbol = null,
                    maxOrdinal: Int = spooky.conf.maxJoinOrdinal,
                    checkpointInterval: Int = spooky.conf.checkpointInterval,
                    numPartitions: Int = spooky.conf.defaultParallelism(this),
                    select: Expression[Any] = null,
                    optimizer: QueryOptimizer = spooky.conf.defaultQueryOptimizer
                    ): PageRowRDD =
    explore(expr, depthKey, maxDepth, ordinalKey, maxOrdinal, checkpointInterval)(
      Visit(new GetExpr(Const.defaultJoinKey), hasTitle),
      numPartitions,
      optimizer = optimizer
    )(Option(select).toSeq: _*)

  def wgetExplore(
                   expr: Expression[Any],
                   hasTitle: Boolean = true,
                   depthKey: Symbol = null,
                   maxDepth: Int = spooky.conf.maxExploreDepth,
                   ordinalKey: Symbol = null,
                   maxOrdinal: Int = spooky.conf.maxJoinOrdinal,
                   checkpointInterval: Int = spooky.conf.checkpointInterval,
                   numPartitions: Int = spooky.conf.defaultParallelism(this),
                   select: Expression[Any] = null,
                   optimizer: QueryOptimizer = spooky.conf.defaultQueryOptimizer
                   ): PageRowRDD =
    explore(expr, depthKey, maxDepth, ordinalKey, maxOrdinal, checkpointInterval)(
      Wget(new GetExpr(Const.defaultJoinKey), hasTitle),
      numPartitions,
      optimizer = optimizer
    )(Option(select).toSeq: _*)
}