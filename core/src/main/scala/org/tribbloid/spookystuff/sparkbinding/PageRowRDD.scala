package org.tribbloid.spookystuff.sparkbinding

import _root_.parquet.org.slf4j.LoggerFactory
import org.apache.spark.HashPartitioner
import org.apache.spark.rdd.{RDD, UnionRDD}
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.storage.StorageLevel
import org.tribbloid.spookystuff.actions._
import org.tribbloid.spookystuff.dsl.{JoinType, _}
import org.tribbloid.spookystuff.entity.PageRow.InMemoryWebCacheRDD
import org.tribbloid.spookystuff.entity._
import org.tribbloid.spookystuff.expressions._
import org.tribbloid.spookystuff.pages.Unstructured
import org.tribbloid.spookystuff.utils._
import org.tribbloid.spookystuff.{views, Const, QueryException, SpookyContext}

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
                           val webCache: InMemoryWebCacheRDD, //used for both quick page lookup from memory and seeding
                           val keys: ListSet[KeyLike],
                           val spooky: SpookyContext,
                           val persisted: ArrayBuffer[RDD[_]]
                           )
  extends RDD[PageRow](self) with PageRowRDDOverrides {

  import RDD._
  import views._

  def this(
            self: RDD[PageRow],
            spooky: SpookyContext
            ) = this(

    self match {
      case self: PageRowRDD => self.self //avoid recursive
      case _ => self
    },
    self.sparkContext.emptyRDD,
    ListSet(), spooky, ArrayBuffer())

  def this(
            self: RDD[PageRow],
            keys: ListSet[KeyLike],
            spooky: SpookyContext
            ) = this(

    self match {
      case self: PageRowRDD => self.self //avoid recursive
      case _ => self
    },
    self.sparkContext.emptyRDD,
    keys, spooky, ArrayBuffer())

  def copy(
            self: RDD[PageRow] = this.self,
            webCache: InMemoryWebCacheRDD = this.webCache,
            keys: ListSet[KeyLike] = this.keys,
            spooky: SpookyContext = this.spooky,
            cachedRDDs: ArrayBuffer[RDD[_]] = this.persisted
            ): PageRowRDD = {

    val result = new PageRowRDD(
      self match {
        case self: PageRowRDD => self.self //avoid recursive
        case _ => self
      },
      webCache,
      keys, spooky, cachedRDDs
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

  private def discardPages: PageRowRDD = this.copy(self = this.map(_.copy(pageLikes = Array())))

  private def discardWebCache: PageRowRDD = {
    //TODO: unpersist it
    this.webCache.unpersist(blocking = false)
    this.copy(webCache = sparkContext.emptyRDD)
  }

  private def discardExploredRows: PageRowRDD = {
    this.copy(webCache = webCache.discardRows)
  }

  def cleanCachedRDDs(): PageRowRDD = {
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

    //    this.persistDuring(spooky.conf.defaultStorageLevel){
    if (this.getStorageLevel == StorageLevel.NONE){
      this.persisted += this.persist(spooky.conf.defaultStorageLevel)
    }

    val result = this.sortBy{_.ordinal(sortKeysSeq)}
    result.count()
    this.cleanCachedRDDs()
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

    val resultRows = this
      .coalesce(numPartitions)
      .flatMap(
        row =>
          _traces
            .interpolate(row)
            .flatMap{
            trace =>
              val pages = trace.resolve(spooky)

              row.putPages(pages, joinType)
          }
      )

    this.copy(resultRows)
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

    val traceToRows: RDD[(Trace, PageRow)] = this.flatMap {
      row =>
        _traces.interpolate(row).map(interpolatedTrace => interpolatedTrace -> row)
    }
      .partitionBy(new HashPartitioner(numPartitions))

    //    val traceToSegment: RDD[(Trace, SegmentID)] = traceToRows.mapValues(_.segment)

    val updated: PageRowRDD = if (!useWebCache) {
      //trace -> Pages -> segments (should only be injected into row if the row's segmentID is contained)
      val pageRows = traceToRows
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
      val webCacheRDD = this.webCache
      val cogrouped: RDD[(DryRun, (Iterable[(Trace, PageRow)], Iterable[SquashedRow]))] =
        traceToRows.keyBy(_._1.dryrun).cogroup(webCacheRDD, numPartitions)

      val newRows_newCache = cogrouped.map {
        triplet =>
          val tuple = triplet._2
          val rows: Iterable[PageRow] = tuple._1.map(_._2)
          assert(tuple._2.size <= 1)
          //tuple._2 should only have one or zero squashedRow
          val squashedRowOption = tuple._2.reduceOption(_ ++ _)

          squashedRowOption match {
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
              val newCached = triplet._1 -> SquashedRow(pageLikes.toArray, seedRowsPost.toArray)

              newSelf -> newCached
            case Some(squashedRow) => //found something, use same pages, add into rows except those found with existing segmentIDs, only add new pages with non-existing segmentIDs into lookup
              val dryrun = triplet._1
              squashedRow.pageLikes.foreach {
                pageLike =>
                  val sameBacktrace = dryrun.find(_ == pageLike.uid.backtrace).get
                  pageLike.uid.backtrace.injectFrom(sameBacktrace)
              }
              val pageLikes = squashedRow.pageLikes
              val fetchedRows = rows.flatMap(_.putPages(pageLikes, joinType))

              val existingSegIDs = squashedRow.rows.map(_.segmentID).toSet
              val seedRows = fetchedRows.filterNot(row => existingSegIDs.contains(row.segmentID))
                .groupBy(_.segmentID)
                .flatMap(tuple => seedFilter(tuple._2))
              val seedRowsPost = seedRows.flatMap(postProcessing)

              val newSelf = if (!seed) fetchedRows.flatMap(postProcessing)
              else seedRowsPost
              val newCached = triplet._1 -> squashedRow.copy(rows = squashedRow.rows ++ seedRowsPost)

              newSelf -> newCached
          }
      }
        .cache()

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

    val _expr = expr defaultAs Symbol(Const.defaultJoinKey)

    var depthFromExclusive = 0

    if (this.getStorageLevel == StorageLevel.NONE) {
      this.persisted += this.persist(spooky.conf.defaultStorageLevel)
    }
    if (this.context.getCheckpointDir.isEmpty) this.context.setCheckpointDir(spooky.conf.dirs.checkpoint)

    val firstResultRDD = this
      .coalesce(numPartitions) //TODO: simplify

    val firstStageRDD = firstResultRDD
      .map {
      row =>
        val seeds = Seq(row)
        val dryruns = row.pageLikes.toSeq.map(_.uid.backtrace).distinct

        ExploreStage(seeds, dryruns = Set(dryruns))
    }

    val resultRDDs = ArrayBuffer[RDD[PageRow]](
      firstResultRDD
        .clearTemp
        .select(Option(depthKey).map(key => Literal(0) ~ key).toSeq: _*)
    )

    val resultKeys = this.keys ++ Seq(TempKey(_expr.name), Key.sortKey(depthKey), Key.sortKey(ordinalKey), Key.sortKey(flattenPagesOrdinalKey)).flatMap(Option(_))

    //    val resultSortKeysSeq: Seq[Key] = resultKeys.toSeq.reverse.flatMap{
    //      case k: Key with SortKey => Some(k)
    //      case _ => None
    //    }

    var stageRDD = firstStageRDD
    while(true) {

      val _depthFromExclusive = depthFromExclusive //var in closure being shipped to workers usually end up miserably (not synched properly)
      val depthToInclusive = Math.min(_depthFromExclusive + checkpointInterval, maxDepth)

      //      assert(_depthFromExclusive < depthToInclusive, _depthFromExclusive.toString+":"+ depthToInclusive)

      val batchExeRDD = stageRDD.map {
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

      this.persisted += batchExeRDD.persist(spooky.conf.defaultStorageLevel)
      batchExeRDD.checkpoint()

      stageRDD = batchExeRDD.map(_._2).filter(_.hasMore) //TODO: repartition to balance?

      val totalRDD = batchExeRDD.flatMap(_._1)
      resultRDDs += totalRDD

      val count = stageRDD.count()
      LoggerFactory.getLogger(this.getClass).info(s"$count segment(s) have uncrawled seed(s) after $depthToInclusive iteration(s)")
      depthFromExclusive = depthToInclusive

      if (count == 0 || depthToInclusive >= maxDepth) return result
    }

    def result: PageRowRDD = {

      val resultSelf = new UnionRDD(this.sparkContext, resultRDDs).coalesce(numPartitions) //TODO: not an 'official' API, and not efficient
      val result = this.copy(self = resultSelf, keys = resultKeys)
      result.select(select: _*)
    }

    result
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

    if (this.getStorageLevel == StorageLevel.NONE) {
      this.persisted += this.persist(spooky.conf.defaultStorageLevel)
    }
    val _expr = expr defaultAs Symbol(Const.defaultJoinKey)

    val depth0 = this.clearTemp.select(Option(depthKey).map(key => Literal(0) ~ key).toSeq: _*)

    val WebCache0 = {
      val webCache: InMemoryWebCacheRDD = if (useWebCache) depth0.webCache
      else sparkContext.emptyRDD

      webCache.indexRows(
        depth0.self,
        rows => rows.headOption
      )
    }
    var seeds = depth0
      .copy(webCache = WebCache0)

    val postProcessing: PageRow => Iterable[PageRow] = {
      row =>
        row.flattenPages(flattenPagesPattern, flattenPagesOrdinalKey)
      //          .flatMap(_.select(select: _*))
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

      val newRowsSize = newRows.count()
      LoggerFactory.getLogger(this.getClass).info(s"found $newRowsSize new row(s) after $depth iterations")

      if (newRowsSize == 0) return result

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