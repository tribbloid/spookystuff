package org.tribbloid.spookystuff.sparkbinding

import org.apache.spark.rdd.RDD
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
import scala.collection.mutable
import scala.language.implicitConversions
import scala.reflect.ClassTag

/**
 * Created by peng on 8/29/14.
 */
case class PageRowRDD(
                       @transient self: RDD[PageRow],
                       @transient keys: ListSet[KeyLike] = ListSet(),
                       @transient indexKeys: ListSet[Key] = ListSet(),
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
        this.indexKeys ++ other.indexKeys.toSeq.reverse
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
        this.indexKeys.intersect(other.indexKeys)
      )
    case _ => super.intersection(other)
  }

  override def intersection(other: RDD[PageRow], numPartitions: Int): PageRowRDD = other match {

    case other: PageRowRDD =>
      this.copy(
        super.intersection(other.self),
        this.keys.intersect(other.keys),
        this.indexKeys.intersect(other.indexKeys)
      )
    case _ => super.intersection(other, numPartitions)
  }
  //-------------------all before this lines are self typed wrappers--------------------

  def toPageRowRDD: PageRowRDD = this

  def toMapRDD: RDD[Map[String, Any]] = this.map(_.asMap())

  def toJSON: RDD[String] = this.map(_.toJSON())

  //TODO: investigate using the new applySchema api to avoid losing type info
  def toSchemaRDD(order: Boolean = true): SchemaRDD = {

    val jsonRDD = this.toJSON

    if (jsonRDD.getStorageLevel == StorageLevel.NONE) jsonRDD.persist(StorageLevel.MEMORY_AND_DISK) //for some unknown reason SQLContext.jsonRDD uses the parameter RDD twice, this has to be fixed by somebody else
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
      val validIndexKeyNames = indexKeys.toSeq
        .filter(key => key.isInstanceOf[Key])
        .map(key => Utils.canonizeColumnName(key.name))
        .filter(name => schemaRDD.schema.fieldNames.contains(name))
      if (validIndexKeyNames.isEmpty) result
      else {
        val indexColumns = validIndexKeyNames.reverse.map(name => Symbol(name))
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
               indexKey: Symbol = null,
               limit: Int = Int.MaxValue,
               left: Boolean = true
               ): PageRowRDD = {
    val selected = this.select(expr)

    val flattened = selected.flatMap(_.flatten(expr.name, Key(indexKey), limit, left))
    selected.copy(
      self = flattened,
      keys = selected.keys ++ Option(Key(indexKey)),
      indexKeys = selected.indexKeys ++ Option(Key(indexKey))
    )
  }

  private def flattenTemp(
                           expr: Expression[Any],
                           indexKey: Symbol = null,
                           limit: Int = Int.MaxValue,
                           left: Boolean = true
                           ): PageRowRDD = {
    val selected = this.selectTemp(expr)

    val flattened = selected.flatMap(_.flatten(expr.name, Key(indexKey), limit, left))
    selected.copy(
      self = flattened,
      keys = selected.keys ++ Option(Key(indexKey)),
      indexKeys = selected.indexKeys ++ Option(Key(indexKey))
    )
  }

  //alias of flatten
  def explode(
               expr: Expression[Any],
               indexKey: Symbol = null,
               limit: Int = Int.MaxValue,
               left: Boolean = true
               ): PageRowRDD = flatten(expr, indexKey, limit, left)

  //  /**
  //   * break each page into 'shards', used to extract structured data from tables
  //   * @param selector denotes enclosing elements of each shards
  //   * @param limit only the first n elements will be used, default to Const.fetchLimit
  //   * @return RDD[Page], each page will generate several shards
  //   */
  def flatSelect(
                  expr: Expression[Seq[Unstructured]], //avoid confusion
                  indexKey: Symbol = null,
                  limit: Int = Int.MaxValue,
                  left: Boolean = true
                  )(exprs: Expression[Any]*) ={

    this
      .flattenTemp(expr defaultAs Symbol(Const.defaultJoinKey), indexKey, limit, left)
      .select(exprs: _*)
      .clearTemp
  }

  def flattenPages(
                    pattern: Symbol = Symbol(Const.onlyPageWildcard), //TODO: enable it
                    indexKey: Symbol = null
                    ): PageRowRDD =
    this.copy(
      self = this.flatMap(_.flattenPages(pattern.name, Key(indexKey))),
      keys = this.keys ++ Option(Key(indexKey)),
      indexKeys = this.indexKeys ++ Option(Key(indexKey))
    )

  def lookup(): RDD[(Trace, PageLike)] = {
    val persisted = if (this.getStorageLevel == StorageLevel.NONE) this.persist(StorageLevel.MEMORY_AND_DISK)
    else this

    persisted
      .flatMap(_.pageLikes.map(page => page.uid.backtrace -> page ))
    //TODO: really takes a lot of space, how to eliminate?
  }

  def fetch(
             traces: Set[Trace],
             joinType: JoinType = Const.defaultJoinType,
             flattenPagesPattern: Symbol = '*, //by default, always flatten all pages
             flattenPagesIndexKey: Symbol = null,
             numPartitions: Int = this.sparkContext.defaultParallelism,
             optimizer: QueryOptimizer = spooky.conf.defaultQueryOptimizer
             ): PageRowRDD = {

    val _traces = traces.autoSnapshot

    spooky.broadcast()

    val result = optimizer match {
      case Minimal =>
        //        if (lookup != null)throw new UnsupportedOperationException(s"${optimizer.getClass.getSimpleName} optimizer can't use lookup table")
        _dumbFetch(_traces, joinType, numPartitions)
      case Smart =>
        _smartFetch(_traces, joinType, numPartitions, lookup())
      case _ => throw new UnsupportedOperationException(s"${optimizer.getClass.getSimpleName} optimizer is not supported in this query")
    }

    if (flattenPagesPattern != null) result.flattenPages(flattenPagesPattern,flattenPagesIndexKey)
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

            val latestBatchOption = PageRow.discoverLatestBlockOutput(lookupPages)

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
            indexKey: Symbol = null, //left & idempotent parameters are missing as they are always set to true
            limit: Int = spooky.conf.joinLimit
            )(
            traces: Set[Trace],
            joinType: JoinType = Const.defaultJoinType,
            numPartitions: Int = this.sparkContext.defaultParallelism,
            flattenPagesPattern: Symbol = '*,
            flattenPagesIndexKey: Symbol = null,
            optimizer: QueryOptimizer = spooky.conf.defaultQueryOptimizer
            )(
            select: Expression[Any]*
            ): PageRowRDD = {

    this
      .flattenTemp(expr defaultAs Symbol(Const.defaultJoinKey), indexKey, limit, left = true)
      .fetch(traces, joinType, flattenPagesPattern, flattenPagesIndexKey, numPartitions, optimizer)
      .select(select: _*)
      .clearTemp
  }

  /**
   * results in a new set of Pages by crawling links on old pages
   * old pages that doesn't contain the link will be ignored
   * @param limit only the first n links will be used, default to Const.fetchLimit
   * @return RDD[Page]
   */
  def visitJoin(
                 expr: Expression[Any],
                 hasTitle: Boolean = true,
                 indexKey: Symbol = null, //left & idempotent parameters are missing as they are always set to true
                 limit: Int = spooky.conf.joinLimit,
                 joinType: JoinType = Const.defaultJoinType,
                 numPartitions: Int = this.sparkContext.defaultParallelism,
                 select: Expression[Any] = null,
                 optimizer: QueryOptimizer = spooky.conf.defaultQueryOptimizer
                 ): PageRowRDD =
    this.join(expr, indexKey, limit)(
      Visit(new GetExpr(Const.defaultJoinKey), hasTitle),
      joinType,
      numPartitions,
      optimizer = optimizer
    )(Option(select).toSeq: _*)

  /**
   * same as join, but avoid launching a browser by using direct http GET (wget) to download new pages
   * much faster and less stressful to both crawling and target server(s)
   * @param limit only the first n links will be used, default to Const.fetchLimit
   * @return RDD[Page]
   */
  def wgetJoin(
                expr: Expression[Any],
                hasTitle: Boolean = true,
                indexKey: Symbol = null, //left & idempotent parameters are missing as they are always set to true
                limit: Int = spooky.conf.joinLimit,
                joinType: JoinType = Const.defaultJoinType,
                numPartitions: Int = this.sparkContext.defaultParallelism,
                select: Expression[Any] = null,
                optimizer: QueryOptimizer = spooky.conf.defaultQueryOptimizer
                ): PageRowRDD =
    this.join(expr, indexKey, limit)(
      Wget(new GetExpr(Const.defaultJoinKey), hasTitle),
      joinType,
      numPartitions,
      optimizer = optimizer
    )(Option(select).toSeq: _*)

  def distinctSignature(
                         ignore: Iterable[Symbol],
                         numPartitions: Int = this.sparkContext.defaultParallelism
                         ): PageRowRDD = {

    val ignoreKeyNames = ignore.map(_.name)
    val withSignatures = this.keyBy(_.signature(ignoreKeyNames))
    val distinct = withSignatures.reduceByKey((row1, row2) => row1).values

    this.copy(
      self = distinct
    )
  }

  /* if the following conditions are met, row will be removed
  * 1. row has identical trace and name
  * 2. the row has identical cells*/
  def subtractSignature(
                         others: RDD[PageRow],
                         ignore: Iterable[Symbol],
                         numPartitions: Int = this.sparkContext.defaultParallelism
                         ): PageRowRDD = {

    val ignoreKeyNames = ignore.map(_.name)
    val otherSignatures = others.map {
      row => row.signature(ignoreKeyNames)
    }.map{
      signature => signature -> null
    }
    val withSignatures = this.map{
      row => row.signature(ignoreKeyNames) -> row
    }
    val excluded = withSignatures.subtractByKey(otherSignatures, numPartitions).values

    this.copy(
      self = excluded
    )
  }

  def explore(
               expr: Expression[Any],
               depthKey: Symbol = null,
               maxDepth: Int = spooky.conf.maxExploreDepth,
               checkpointInterval: Int = 20
               )(
               traces: Set[Trace],
               numPartitions: Int = this.sparkContext.defaultParallelism,
               flattenPagesPattern: Symbol = '*,
               flattenPagesIndexKey: Symbol = null,
               optimizer: QueryOptimizer = spooky.conf.defaultQueryOptimizer
               )(
               select: Expression[Any]*
               ): PageRowRDD = {

    val _traces = traces.autoSnapshot

    spooky.broadcast()

    optimizer match {
      case Minimal =>
        _dumbExplore(expr, depthKey, maxDepth, checkpointInterval)(_traces, numPartitions, flattenPagesPattern, flattenPagesIndexKey)(select: _*)
      case Smart =>
        _smartExplore(expr, depthKey, maxDepth, checkpointInterval)(_traces, numPartitions, flattenPagesPattern, flattenPagesIndexKey)(select: _*)
      case _ => throw new UnsupportedOperationException(s"${optimizer.getClass.getSimpleName} optimizer is not supported in this query")
    }
  }

  //this is a single-threaded explore, of which implementation is similar to good old pagination.
  private def _dumbExplore(
                            expr: Expression[Any],
                            depthKey: Symbol = null,
                            maxDepth: Int,
                            checkpointInterval: Int
                            )(
                            _traces: Set[Trace],
                            numPartitions: Int,
                            flattenPagesPattern: Symbol,
                            flattenPagesIndexKey: Symbol
                            )(
                            select: Expression[Any]*
                            ): PageRowRDD = {

    val spooky = this.spooky

    val _expr = expr defaultAs Symbol(Const.defaultJoinKey)

    val beforeSelectSelf = this.flatMap{
      row =>
        val seeds = row.select(Literal(0) ~ depthKey).toSeq

        val excludeTraces = mutable.HashSet[Trace]()

        val excludeDryruns = row
          .pageLikes
          .map(_.uid)
          .groupBy(_.backtrace)
          .filter{
          tuple =>
            tuple._2.size == tuple._2.head.total
        }
          .keys.toSeq

        val tuple = PageRow.dumbExplore(
          seeds,
          excludeTraces,
          excludeDryruns = Set(excludeDryruns)
        )(
            _expr,
            depthKey,
            maxDepth,
            spooky
          )(
            _traces,
            flattenPagesPattern,
            flattenPagesIndexKey
          )

        tuple._1
    }

    val beforeSelectKeys = this.keys ++ Seq(TempKey(_expr.name), Key(depthKey), Key(flattenPagesIndexKey)).flatMap(Option(_))
    val beforeSelectIndexKeys = this.indexKeys ++ Seq(Key(depthKey), Key(flattenPagesIndexKey)).flatMap(Option(_))

    val beforeSelect = this.copy(self = beforeSelectSelf, keys = beforeSelectKeys, indexKeys = beforeSelectIndexKeys)

    val result = beforeSelect
      .select(select: _*)
      .clearTemp

    result
  }

  //recursive join and union! applicable to many situations like (wide) pagination and deep crawling
  private def _smartExplore(
                             expr: Expression[Any],
                             depthKey: Symbol,
                             maxDepth: Int,
                             checkpointInterval: Int
                             )(
                             _traces: Set[Trace],
                             numPartitions: Int,
                             flattenPagesPattern: Symbol,
                             flattenPagesIndexKey: Symbol
                             )(
                             select: Expression[Any]*
                             ): PageRowRDD = {

    val spooky = this.spooky

    var newRows = this

    var total = if (depthKey != null)
      this.select(Literal(0) ~ depthKey)
        .copy(indexKeys = this.indexKeys + Key(depthKey))
    else this

    var lookups: RDD[(Trace, PageLike)] =this.lookup()

    if (this.context.getCheckpointDir.isEmpty) this.context.setCheckpointDir(spooky.conf.dirs.checkpoint)

    val _expr = expr defaultAs Symbol(Const.defaultJoinKey)

    for (depth <- 1 to maxDepth) {
      //ALWAYS inner join
      val joined = newRows
        .flattenTemp(_expr, null, Int.MaxValue, left = true)
        ._smartFetch(_traces, Inner, numPartitions, lookups)

      val newLookups = joined.lookup()

      lookups = lookups.union(newLookups) //TODO: remove pages with identical uid but different _traces? or set the lookup table as backtrace -> seq directly.

      if (depth % checkpointInterval == 0) {
        lookups.checkpoint()
        val size = lookups.count()
      }

      val flattened = joined.flattenPages(flattenPagesPattern, flattenPagesIndexKey)

      val allIgnoredKeys = Seq(depthKey, flattenPagesIndexKey).filter(_ != null)

      newRows = flattened
        .subtractSignature(total, allIgnoredKeys)
        .distinctSignature(allIgnoredKeys)
        .persist(StorageLevel.MEMORY_AND_DISK)

      if (depth % checkpointInterval == 0) {
        newRows.checkpoint()
      }

      val newRowsSize = newRows.count()
      LoggerFactory.getLogger(this.getClass).info(s"found $newRowsSize new row(s) at $depth depth")

      if (newRowsSize == 0){
        return total
          .select(select: _*)
          .clearTemp
          .coalesce(numPartitions) //early stop condition if no new pages with the same data is detected
      }

      total = total.union(
        if (depthKey != null) newRows.select(new Literal(depth) ~ depthKey)
        else newRows
      ).coalesce(numPartitions)
      if (depth % checkpointInterval == 0) {
        total.checkpoint()
        val size = total.count()
      }
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
                    numPartitions: Int = this.sparkContext.defaultParallelism,
                    select: Expression[Any] = null,
                    optimizer: QueryOptimizer = spooky.conf.defaultQueryOptimizer
                    ): PageRowRDD =
    explore(expr, depthKey, maxDepth)(
      Visit(new GetExpr(Const.defaultJoinKey), hasTitle),
      numPartitions,
      optimizer = optimizer
    )(Option(select).toSeq: _*)

  def wgetExplore(
                   expr: Expression[Any],
                   hasTitle: Boolean = true,
                   depthKey: Symbol = null,
                   maxDepth: Int = spooky.conf.maxExploreDepth,
                   numPartitions: Int = this.sparkContext.defaultParallelism,
                   select: Expression[Any] = null,
                   optimizer: QueryOptimizer = spooky.conf.defaultQueryOptimizer
                   ): PageRowRDD =
    explore(expr, depthKey, maxDepth)(
      Wget(new GetExpr(Const.defaultJoinKey), hasTitle),
      numPartitions,
      optimizer = optimizer
    )(Option(select).toSeq: _*)

  /**
   * insert many pages for each old page by recursively visiting "next page" link
   * @param selector selector of the "next page" element
   * @param limit depth of recursion
   * @return RDD[Page], contains both old and new pages
   */
  @Deprecated def paginate(
                            selector: String,
                            attr: String = "abs:href",
                            wget: Boolean = true,
                            postAction: Seq[Action] = Seq()
                            )(
                            limit: Int = spooky.conf.paginationLimit,
                            indexKey: Symbol = null,
                            flatten: Boolean = true,
                            last: Boolean = false
                            ): PageRowRDD = {

    val spooky = this.spooky.broadcast()

    val realIndexKey = Key(indexKey)

    val result = this.flatMap {
      _.paginate(selector, attr, wget, postAction)(limit, Key(indexKey), flatten)(spooky)
    }

    this.copy(
      self = result,
      keys = this.keys ++ Option(realIndexKey),
      indexKeys = this.indexKeys ++ Option(Key(indexKey))
    )
  }
}