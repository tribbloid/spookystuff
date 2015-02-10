package org.tribbloid.spookystuff.sparkbinding

import org.apache.spark.rdd.{RDD, UnionRDD}
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{Partition, TaskContext}
import org.slf4j.LoggerFactory
import org.tribbloid.spookystuff.actions._
import org.tribbloid.spookystuff.dsl.{Inner, JoinType, _}
import org.tribbloid.spookystuff.entity._
import org.tribbloid.spookystuff.expressions._
import org.tribbloid.spookystuff.pages.{PageLike, Unstructured}
import org.tribbloid.spookystuff.utils._
import org.tribbloid.spookystuff.{Const, SpookyContext}

import scala.collection.immutable.ListSet
import scala.collection.mutable.ArrayBuffer
import scala.language.implicitConversions
import scala.reflect.ClassTag

/**
 * Created by peng on 8/29/14.
 */
case class PageRowRDD(
                       @transient self: RDD[PageRow],
                       @transient keys: ListSet[KeyLike] = ListSet(),
                       @transient sortKeys: ListSet[Key] = ListSet(),
                       spooky: SpookyContext
                       )
  extends RDD[PageRow](self) {

  import org.apache.spark.SparkContext._

  override def getPartitions: Array[Partition] = firstParent[PageRow].partitions

  override val partitioner = self.partitioner

  override def compute(split: Partition, context: TaskContext) =
    firstParent[PageRow].iterator(split, context)
  //-----------------------------------------------------------------------

  private implicit def selfToPageRowRDD(self: RDD[PageRow]): PageRowRDD = this.copy(self = self)

  override def filter(f: PageRow => Boolean): PageRowRDD = super.filter(f)

  override def distinct(): PageRowRDD = super.distinct()

  override def distinct(numPartitions: Int)(implicit ord: Ordering[PageRow] = null): PageRowRDD =
    super.distinct(numPartitions)(ord)

  override def repartition(numPartitions: Int)(implicit ord: Ordering[PageRow] = null): PageRowRDD =
    super.repartition(numPartitions)(ord)

  override def coalesce(numPartitions: Int, shuffle: Boolean = false)(implicit ord: Ordering[PageRow] = null): PageRowRDD =
    super.coalesce(numPartitions, shuffle)(ord)

  override def sample(withReplacement: Boolean,
                      fraction: Double,
                      seed: Long = Utils.random.nextLong()): PageRowRDD =
    super.sample(withReplacement, fraction, seed)

  override def union(other: RDD[PageRow]): PageRowRDD = other match {

    case other: PageRowRDD =>
      this.copy(
        super.union(other.self),
        this.keys ++ other.keys.toSeq.reverse,
        this.sortKeys ++ other.sortKeys.toSeq.reverse
      )
    case _ => super.union(other)
  }

  override def ++(other: RDD[PageRow]): PageRowRDD = this.union(other)

  override def sortBy[K](
                          f: (PageRow) => K,
                          ascending: Boolean = true,
                          numPartitions: Int = this.partitions.size)
                        (implicit ord: Ordering[K], ctag: ClassTag[K]): PageRowRDD = super.sortBy(f, ascending, numPartitions)(ord, ctag)

  override def intersection(other: RDD[PageRow]): PageRowRDD = other match {

    case other: PageRowRDD =>
      this.copy(
        super.intersection(other.self),
        this.keys.intersect(other.keys),//TODO: need validation that it won't change sequence
        this.sortKeys.intersect(other.sortKeys)
      )
    case _ => super.intersection(other)
  }

  override def intersection(other: RDD[PageRow], numPartitions: Int): PageRowRDD = other match {

    case other: PageRowRDD =>
      this.copy(
        super.intersection(other.self),
        this.keys.intersect(other.keys),
        this.sortKeys.intersect(other.sortKeys)
      )
    case _ => super.intersection(other, numPartitions)
  }
  //-------------------all before this lines are self typed wrappers--------------------

  def generateGroupID: PageRowRDD = this.copy(self = this.map(_.generateGroupID))

  def toPageRowRDD: PageRowRDD = this

  def toMapRDD: RDD[Map[String, Any]] = this.map(_.asMap())

  def toJSON: RDD[String] = this.map(_.toJSON)

  //TODO: investigate using the new applySchema api to avoid losing type info
  def toSchemaRDD(order: Boolean = true): SchemaRDD = {

    val jsonRDD = this.toJSON

    if (jsonRDD.getStorageLevel == StorageLevel.NONE) jsonRDD.persist(StorageLevel.MEMORY_AND_DISK_SER) //for some unknown reason SQLContext.jsonRDD uses the parameter RDD twice, this has to be fixed by somebody else
    //TODO: unpersist after next action, is it even possible?

    import spooky.sqlContext._
    val schemaRDD = this.spooky.sqlContext.jsonRDD(jsonRDD)

    val validKeyNames = keys.toSeq
      .filter(key => key.isInstanceOf[Key])
      .map(key => Utils.canonizeColumnName(key.name))
      .filter(name => schemaRDD.schema.fieldNames.contains(name))
    val columns = validKeyNames.reverse.map(name => UnresolvedAttribute(name))

    val result = schemaRDD
      .select(columns: _*)

    if (!order) result
    else {
      val validOrdinalKeyNames = sortKeys.toSeq
        .filter(key => key.isInstanceOf[Key])
        .map(key => Utils.canonizeColumnName(key.name))
        .filter(name => schemaRDD.schema.fieldNames.contains(name))
      if (validOrdinalKeyNames.isEmpty) result
      else {
        val indexColumns = validOrdinalKeyNames.reverse.map(name => Symbol(name))
        result.orderBy(indexColumns.map(_.asc): _*)
      }
    }
  }

  def toCSV(separator: String = ","): RDD[String] = this.toSchemaRDD().map {
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

    val spooky = this.spooky.broadcast()

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

    val _exprs = exprs

    val newKeys: Seq[Key] = _exprs.map {
      expr =>
        val key = Key(expr.name)
        assert(!this.keys.contains(key) || expr.isInstanceOf[PlusExpr[_]]) //can't insert the same key twice
        key
    }

    val result = this.copy(
      self = this.flatMap(_.select(_exprs: _*)),
      keys = this.keys ++ newKeys
    )
    result
  }

  private def selectTemp(exprs: Expression[Any]*): PageRowRDD = {

    val _exprs = exprs

    val newKeys: Seq[TempKey] = _exprs.map {
      expr =>
        val key = TempKey(expr.name)
        key
    }

    this.copy(
      self = this.flatMap(_.selectTemp(_exprs: _*)),
      keys = this.keys ++ newKeys
    )
  }

  def remove(keys: Symbol*): PageRowRDD = {
    val names = keys.map(key => Key(key))
    this.copy(
      self = this.map(_.remove(names)),
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

    val flattened = selected.flatMap(_.flatten(expr.name, Key(ordinalKey), maxOrdinal, left))
    selected.copy(
      self = flattened,
      keys = selected.keys ++ Option(Key(ordinalKey)),
      sortKeys = selected.sortKeys ++ Option(Key(ordinalKey))
    )
  }

  private def flattenTemp(
                           expr: Expression[Any],
                           ordinalKey: Symbol = null,
                           maxOrdinal: Int = Int.MaxValue,
                           left: Boolean = true
                           ): PageRowRDD = {
    val selected = this.selectTemp(expr)

    val flattened = selected.flatMap(_.flatten(expr.name, Key(ordinalKey), maxOrdinal, left))
    selected.copy(
      self = flattened,
      keys = selected.keys ++ Option(Key(ordinalKey)),
      sortKeys = selected.sortKeys ++ Option(Key(ordinalKey))
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
      .clearTemp
  }

  def flattenPages(
                    pattern: Symbol = Symbol(Const.onlyPageWildcard), //TODO: enable it
                    ordinalKey: Symbol = null
                    ): PageRowRDD =
    this.copy(
      self = this.flatMap(_.flattenPages(pattern.name, Key(ordinalKey))),
      keys = this.keys ++ Option(Key(ordinalKey)),
      sortKeys = this.sortKeys ++ Option(Key(ordinalKey))
    )

  def lookup(): RDD[(Trace, PageLike)] = {
    val persisted = if (this.getStorageLevel == StorageLevel.NONE) this.persist(StorageLevel.MEMORY_AND_DISK_SER)
    else this

    persisted.flatMap(_.pageLikes.map(page => page.uid.backtrace -> page ))
    //TODO: really takes a lot of space, how to eliminate?
  }

  def fetch(
             traces: Set[Trace],
             joinType: JoinType = Const.defaultJoinType,
             flattenPagesPattern: Symbol = '*, //by default, always flatten all pages
             flattenPagesOrdinalKey: Symbol = null,
             numPartitions: Int = spooky.conf.defaultParallelism(this),
             optimizer: QueryOptimizer = spooky.conf.defaultQueryOptimizer
             ): PageRowRDD = {

    val _traces = traces.autoSnapshot

    spooky.broadcast()

    val _result = optimizer match {
      case Narrow =>
        //        if (lookup != null)throw new UnsupportedOperationException(s"${optimizer.getClass.getSimpleName} optimizer can't use lookup table")
        _dumbFetch(_traces, joinType, numPartitions)
      case WideNoLookup =>
        _smartFetch(_traces, joinType, numPartitions, null)
      case Wide =>
        _smartFetch(_traces, joinType, numPartitions, lookup())
      case _ => throw new UnsupportedOperationException(s"${optimizer.getClass.getSimpleName} optimizer is not supported in this query")
    }

    val result = _result.generateGroupID

    if (flattenPagesPattern != null) result.flattenPages(flattenPagesPattern,flattenPagesOrdinalKey)
    else result
  }

  private def _dumbFetch(
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

  private def _smartFetch(
                           _traces: Set[Trace],
                           joinType: JoinType,
                           numPartitions: Int,
                           lookup: RDD[(Trace, PageLike)]
                           ): PageRowRDD = {

    val spooky = this.spooky

    val traceToRow = this.flatMap {
      row =>
        _traces.interpolate(row).map(interpolatedTrace => interpolatedTrace -> row)
    }

    val squashes = traceToRow.groupByKey(numPartitions).map(tuple => Squash(tuple._1, tuple._2))

    val resultRows: RDD[PageRow] = if (lookup == null) {
      //no lookup
      squashes.flatMap(_.resolveAndPut(joinType, spooky))
    }
    else {
      //lookup
      val backtraceToSquashWithIndex = squashes.flatMap{
        //key not unique, different trace may yield to same backtrace.
        squash =>
          val dryruns = squash.trace.dryrun.zipWithIndex
          if (dryruns.nonEmpty) dryruns.map(tuple => tuple._1 -> (squash, tuple._2))
          else Seq(null.asInstanceOf[Trace] -> (squash, -1))
      }

      val cogrouped = backtraceToSquashWithIndex
        .cogroup(lookup)

      val squashToIndexWithPagesOption = cogrouped.flatMap{
        triplet =>
          val backtrace = triplet._1
          val tuple = triplet._2
          val squashedWithIndexes = tuple._1
          if (squashedWithIndexes.isEmpty) {
            Seq()
          }
          else if (backtrace == null) {
            squashedWithIndexes.map{
              squashedWithIndex =>
                squashedWithIndex._1 -> (squashedWithIndex._2, Some(Seq()))
            }
          }
          else {
            val lookupPages = tuple._2
            lookupPages.foreach(_.uid.backtrace.injectFrom(backtrace))

            val latestBatchOption = PageRow.discoverLatestBatch(lookupPages)

            squashedWithIndexes.map{
              squashedWithIndex =>
                squashedWithIndex._1 -> (squashedWithIndex._2, latestBatchOption)
            }
          }
      }

      val result = squashToIndexWithPagesOption.groupByKey(numPartitions).flatMap{
        tuple =>
          val squash = tuple._1
          val IndexWithPageOptions = tuple._2.toSeq
          if (IndexWithPageOptions.map(_._2).contains(None)) squash.resolveAndPut(joinType, spooky)
          else {
            val pages = IndexWithPageOptions.sortBy(_._1).flatMap(_._2.get)
            squash.rows.flatMap(_.putPages(pages, joinType))
          }
      }

      result
    }

    this.copy(resultRows)
  }

  def join(
            expr: Expression[Any], //name is discarded
            ordinalKey: Symbol = null, //left & idempotent parameters are missing as they are always set to true
            maxOrdinal: Int = spooky.conf.maxJoinOrdinal
            )(
            traces: Set[Trace],
            joinType: JoinType = Const.defaultJoinType,
            numPartitions: Int = spooky.conf.defaultParallelism(this),
            flattenPagesPattern: Symbol = '*,
            flattenPagesOrdinalKey: Symbol = null,
            optimizer: QueryOptimizer = spooky.conf.defaultQueryOptimizer
            )(
            select: Expression[Any]*
            ): PageRowRDD = {

    this
      .flattenTemp(expr defaultAs Symbol(Const.defaultJoinKey), ordinalKey, maxOrdinal, left = true)
      .fetch(traces, joinType, flattenPagesPattern, flattenPagesOrdinalKey, numPartitions, optimizer)
      .select(select: _*)
      .clearTemp
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

  /* if the following conditions are met, row will be removed
* 1. row has identical trace and name
* 2. the row has identical cells*/
  private def subtractSignature(
                                 another: RDD[PageRow],
                                 numPartitions: Int
                                 ): PageRowRDD = {

    val otherSignatures = another.map {
      row => row.signature
    }.map{
      signature => signature -> null
    }
    val withSignatures = this.map{
      row => row.signature -> row
    }
    val excluded = withSignatures.subtractByKey(otherSignatures, numPartitions).values

    this.copy(
      self = excluded
    )
  }

  private def distinctSignature(
                                 numPartitions: Int
                                 ): PageRowRDD = {

    val withSignatures = this.keyBy(_.signature)
    val distinct = withSignatures.reduceByKey((row1, row2) => row1).values

    this.copy(
      self = distinct
    )
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
               flattenPagesPattern: Symbol = '*,
               flattenPagesOrdinalKey: Symbol = null,
               optimizer: QueryOptimizer = spooky.conf.defaultQueryOptimizer
               )(
               select: Expression[Any]*
               ): PageRowRDD = {

    val _traces = traces.autoSnapshot

    spooky.broadcast()

    val result = optimizer match {
      case Narrow =>
        _dumbExplore(expr, depthKey, maxDepth, ordinalKey, maxOrdinal, checkpointInterval)(_traces, numPartitions, flattenPagesPattern, flattenPagesOrdinalKey)(select: _*)
      case WideNoLookup =>
        _smartNoLookupExplore(expr, depthKey, maxDepth, ordinalKey, maxOrdinal, checkpointInterval)(_traces, numPartitions, flattenPagesPattern, flattenPagesOrdinalKey)(select: _*)
      case Wide =>
        _smartExplore(expr, depthKey, maxDepth, ordinalKey, maxOrdinal, checkpointInterval)(_traces, numPartitions, flattenPagesPattern, flattenPagesOrdinalKey)(select: _*)
      case _ => throw new UnsupportedOperationException(s"${optimizer.getClass.getSimpleName} optimizer is not supported in this query")
    }

    result.generateGroupID
  }

  //this is a single-threaded explore, of which implementation is similar to good old pagination.
  private def _dumbExplore(
                            expr: Expression[Any],
                            depthKey: Symbol,
                            maxDepth: Int,
                            ordinalKey: Symbol,
                            maxOrdinal: Int,
                            checkpointInterval: Int
                            )(
                            _traces: Set[Trace],
                            numPartitions: Int,
                            flattenPagesPattern: Symbol,
                            flattenPagesOrdinalKey: Symbol
                            )(
                            select: Expression[Any]*
                            ): PageRowRDD = {

    val batchSize = this.spooky.conf.batchSize

    val spooky = this.spooky

    val _expr = expr defaultAs Symbol(Const.defaultJoinKey)

    var depthStart = 0

    val firstResultRDD = this.coalesce(numPartitions).persist(StorageLevel.MEMORY_AND_DISK_SER)

    val firstStageRDD = firstResultRDD
      .map {
      row =>
        val seeds = row.select(Literal(0) ~ depthKey)
        val dryruns = row
          .pageLikes
          .map(_.uid)
          .groupBy(_.backtrace)
          .filter{
          tuple =>
            tuple._2.size == tuple._2.head.blockTotal //I hope this is sufficient condition
        }
          .keys.toSeq

        ExploreStage(seeds, dryruns = Set(dryruns))
    }

    val resultRDDs = ArrayBuffer[RDD[PageRow]](firstResultRDD.select(Literal(0) ~ depthKey))

    var done = false
    var stageRDD = firstStageRDD
    while(!done) {

      val depthEnd = Math.min(depthStart + batchSize, maxDepth)

      val batchExeRDD = stageRDD.map {
        stage =>
          PageRow.dumbExplore(
            stage
          )(
              _expr,
              depthKey,
              depthStart,
              depthEnd,
              maxOrdinal,
              spooky
            )(
              _traces,
              flattenPagesPattern,
              flattenPagesOrdinalKey
            )
      }.persist(StorageLevel.MEMORY_AND_DISK_SER) // change to checkpoint?

      val count = batchExeRDD.count()

      LoggerFactory.getLogger(this.getClass).info(s"$count groups have found new rows after $depthEnd iterations")
      depthStart = depthEnd

      if (count == 0 || depthEnd == maxDepth) done = true

      stageRDD = batchExeRDD.map(_._2).filter(_.hasMore)

      val totalRDD = batchExeRDD.flatMap(_._1)

      resultRDDs += totalRDD
    }

    val resultSelf = new UnionRDD(this.sparkContext, resultRDDs).coalesce(numPartitions) //TODO: not an 'official' API

    val resultKeys = this.keys ++ Seq(TempKey(_expr.name), Key(depthKey), Key(flattenPagesOrdinalKey)).flatMap(Option(_))
    val resultOrdinalKeys = this.sortKeys ++ Seq(Key(depthKey), Key(flattenPagesOrdinalKey)).flatMap(Option(_))

    val result = this.copy(self = resultSelf, keys = resultKeys, sortKeys = resultOrdinalKeys)

    result
      .select(select: _*)
      .clearTemp
  }

  //no lookup, purely relies on cache to avoid redundant fetch
  private def _smartNoLookupExplore(
                                     expr: Expression[Any],
                                     depthKey: Symbol,
                                     maxDepth: Int,
                                     ordinalKey: Symbol,
                                     maxOrdinal: Int,
                                     checkpointInterval: Int
                                     )(
                                     _traces: Set[Trace],
                                     numPartitions: Int,
                                     flattenPagesPattern: Symbol,
                                     flattenPagesOrdinalKey: Symbol
                                     )(
                                     select: Expression[Any]*
                                     ): PageRowRDD = {

    val spooky = this.spooky

    var newRows = this

    var total = if (depthKey != null)
      this.select(Literal(0) ~ depthKey)
        .copy(sortKeys = this.sortKeys + Key(depthKey))
    else this

    if (this.context.getCheckpointDir.isEmpty) this.context.setCheckpointDir(spooky.conf.dirs.checkpoint)

    val _expr = expr defaultAs Symbol(Const.defaultJoinKey)

    for (depth <- 1 to maxDepth) {
      //ALWAYS inner join
      val fetched = newRows
        .flattenTemp(_expr, ordinalKey, maxOrdinal, left = true)
        ._smartFetch(_traces, Inner, numPartitions, null)

      val flattened = fetched.flattenPages(flattenPagesPattern, flattenPagesOrdinalKey)

      newRows = flattened
        .distinctSignature(numPartitions)
        .subtractSignature(total, numPartitions)
        .persist(StorageLevel.MEMORY_AND_DISK_SER)

      if (depth % checkpointInterval == 0) {
        newRows.checkpoint()
      }

      val newRowsSize = newRows.count()
      LoggerFactory.getLogger(this.getClass).info(s"found $newRowsSize new row(s) after $depth iterations")

      if (newRowsSize == 0){
        return total
          .select(select: _*)
          .clearTemp
          .coalesce(numPartitions) //early stop condition if no new pages with the same data is detected
      }

      total = total.union(
        if (depthKey != null) newRows.select(new Literal(depth) ~ depthKey)
        else newRows
      )
    }

    total
      .select(select: _*)
      .clearTemp
      .coalesce(numPartitions)
  }

  //recursive join and union! applicable to many situations like (wide) pagination and deep crawling
  private def _smartExplore(
                             expr: Expression[Any],
                             depthKey: Symbol,
                             maxDepth: Int,
                             ordinalKey: Symbol,
                             maxOrdinal: Int,
                             checkpointInterval: Int
                             )(
                             _traces: Set[Trace],
                             numPartitions: Int,
                             flattenPagesPattern: Symbol,
                             flattenPagesOrdinalKey: Symbol
                             )(
                             select: Expression[Any]*
                             ): PageRowRDD = {

    val spooky = this.spooky

    var newRows = this

    var total = if (depthKey != null)
      this.select(Literal(0) ~ depthKey)
        .copy(sortKeys = this.sortKeys + Key(depthKey))
    else this

    var lookups: RDD[(Trace, PageLike)] =this.lookup()

    if (this.context.getCheckpointDir.isEmpty) this.context.setCheckpointDir(spooky.conf.dirs.checkpoint)

    val _expr = expr defaultAs Symbol(Const.defaultJoinKey)

    for (depth <- 1 to maxDepth) {
      //ALWAYS inner join
      val fetched = newRows
        .flattenTemp(_expr, ordinalKey, maxOrdinal, left = true)
        ._smartFetch(_traces, Inner, numPartitions, lookups)

      val newLookups = fetched.lookup()

      lookups = lookups.union(newLookups) //TODO: remove pages with identical uid but different _traces? or set the lookup table as backtrace -> seq directly.

      if (depth % checkpointInterval == 0) {
        lookups.checkpoint()
        val size = lookups.count()
      }

      val flattened = fetched.flattenPages(flattenPagesPattern, flattenPagesOrdinalKey)

      newRows = flattened
        .distinctSignature(numPartitions)
        .subtractSignature(total, numPartitions)
        .persist(StorageLevel.MEMORY_AND_DISK_SER)

      if (depth % checkpointInterval == 0) {
        newRows.checkpoint()
      }

      val newRowsSize = newRows.count()
      LoggerFactory.getLogger(this.getClass).info(s"found $newRowsSize new row(s) after $depth iterations")

      if (newRowsSize == 0){
        return total
          .select(select: _*)
          .clearTemp
          .coalesce(numPartitions) //early stop condition if no new pages with the same data is detected
      }

      total = total.union(
        if (depthKey != null) newRows.select(new Literal(depth) ~ depthKey)
        else newRows
      )
    }

    total
      .select(select: _*)
      .clearTemp
      .coalesce(numPartitions)
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