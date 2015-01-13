package org.tribbloid.spookystuff.sparkbinding

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.{Partition, TaskContext}
import org.slf4j.LoggerFactory
import org.tribbloid.spookystuff.actions._
import org.tribbloid.spookystuff.dsl.{Inner, JoinType, _}
import org.tribbloid.spookystuff.entity._
import org.tribbloid.spookystuff.expressions._
import org.tribbloid.spookystuff.pages.{PageUtils, PageLike}
import org.tribbloid.spookystuff.utils._
import org.tribbloid.spookystuff.{Const, SpookyContext}

import scala.collection.immutable.ListSet
import scala.language.implicitConversions
import scala.reflect.ClassTag

/**
 * Created by peng on 8/29/14.
 */
case class PageRowRDD(
                       self: RDD[PageRow],
                       @transient keys: ListSet[KeyLike] = ListSet(),
                       @transient indexKeys: ListSet[Key] = ListSet(),
                       @transient spooky: SpookyContext
                       )
  extends RDD[PageRow](self) {

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

  def asMapRDD: RDD[Map[String, Any]] = this.map(_.asMap())

  def asJsonRDD: RDD[String] = this.map(_.asJson())

  //TODO: use the new applySchema api to avoid losing type info
  def asSchemaRDD(): SchemaRDD = {

    val jsonRDD = this.asJsonRDD

    jsonRDD.persist() //for some unknown reason SQLContext.jsonRDD uses the parameter RDD twice, this has to be fixed by somebody else

    //by default, will order the columns to be identical to the sequence they are extracted, data input will be ignored

    val columns = keys.toSeq.filter(_.isInstanceOf[Key]).reverse.map(key => UnresolvedAttribute(Utils.canonizeColumnName(key.name)))

    import spooky.sqlContext._
    //TODO: handle missing columns
    val result = this.spooky.sqlContext.jsonRDD(jsonRDD)
      .select(columns: _*)

    if (indexKeys.isEmpty) result
    else {
      val indexColumns = indexKeys.toSeq.reverse.map(key => Symbol(key.name))
      result.orderBy(indexColumns.map(_.asc): _*)
    }
  }

  def asCsvRDD(separator: String = ","): RDD[String] = this.asSchemaRDD().map {
    _.mkString(separator)
  }

  def asTsvRDD(): RDD[String] = this.asCsvRDD("\t")

  //  def groupByData(): PageSchemaRDD = {
  //    import org.apache.spark.SparkContext._
  //
  //    val result = this.map(row => (row.cells, row.pages))
  //      .groupByKey()
  //      .mapValues(pages => pages.flatMap(itr => itr))
  //      .map(tuple => PageRow(tuple._1, tuple._2.toSeq))
  //
  //    this.copy(
  //      self = result
  //    )
  //  }

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
  def save(
            path: Expression[Any],
            name: Symbol = null,
            overwrite: Boolean = false
            ): PageRowRDD = {

    val spookyBroad = this.context.broadcast(this.spooky)
    def _spooky: SpookyContext = spookyBroad.value

    val saved = this.map {

      pageRow =>
        path(pageRow).foreach {
          str =>
            val strCanon = str
            val page =
              if (name == null) pageRow.getOnlyPage
              else pageRow.getPage(name.name)

            page.foreach(_.save(Seq(strCanon.toString), overwrite)(_spooky))
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
  def dump(
            path: Expression[String] = null,
            name: Symbol = null,
            overwrite: Boolean = false
            ): Array[String] = this.save(path, name, overwrite).flatMap {
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
        assert(!this.keys.contains(key)) //can't insert the same key twice
        key
    }

    val result = this.copy(
      self = this.map(_.select(_exprs)),
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
      self = this.map(_.selectTemp(_exprs)),
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
      self = this.map(_.filterKeys(!_.isInstanceOf[TempKey])),
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
                  expr: Expression[Any],
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
                    pattern: Symbol = '*, //TODO: enable it
                    indexKey: Symbol = null
                    ): PageRowRDD =
    this.copy(
      self = this.flatMap(_.flattenPages(pattern.name, Key(indexKey))),
      keys = this.keys ++ Option(Key(indexKey)),
      indexKeys = this.indexKeys ++ Option(Key(indexKey))
    )

  /**
   * parallel execution in browser(s) to yield a set of web pages
   * each ActionPlan may yield several pages in a row, depending on the number of Export(s) in it
   * @return RDD[Page] as results of execution
   */

  //this won't merge identical traces, only used in case each resolve may yield different result
  def dumbFetch(
                 traces: Set[Trace],
                 joinType: JoinType = Const.defaultJoinType,
                 numPartitions: Int = this.sparkContext.defaultParallelism,
                 flattenPagesPattern: Symbol = '*, //by default, always flatten all pages
                 flattenPagesIndexKey: Symbol = null
                 ): PageRowRDD = {

    val _trace = traces.autoSnapshot
    val withTrace = this.flatMap(
      row => _trace.interpolate(row).map(_ -> row)
    )

    val spookyBroad = this.context.broadcast(this.spooky)
    def _spooky: SpookyContext = spookyBroad.value

    val withPages = withTrace.map{
      tuple => tuple._1.resolve(_spooky) -> tuple._2
    }
    val result = this.copy(self = withPages.flatMap(tuple => tuple._2.putPages(tuple._1, joinType)))

    if (flattenPagesPattern != null) result.flattenPages(flattenPagesPattern,flattenPagesIndexKey)
    else result
  }

  /**
   * smart execution: group identical ActionPlans, execute in parallel, and duplicate result pages to match their original contexts
   * reduce workload by avoiding repeated access to the same url caused by duplicated context or diamond links (A->B,A->C,B->D,C->D)
   * recommended for most cases, mandatory for RDD[ActionPlan] with high duplicate factor, only use !=!() if you are sure that duplicate doesn't exist.
   * @return RDD[Page] as results of execution
   */
  def fetch(
             traces: Set[Trace],
             joinType: JoinType = Const.defaultJoinType,
             numPartitions: Int = this.sparkContext.defaultParallelism,
             flattenPagesPattern: Symbol = '*, //by default, always flatten all pages
             flattenPagesIndexKey: Symbol = null,
             lookupFrom: RDD[PageLike] = this.flatMap(_.pageLikes)
             //           lookupFrom: RDD[PageLike] = null
             ): PageRowRDD = {

    val spookyBroad = this.context.broadcast(this.spooky)

    val result = _fetch(traces, joinType, numPartitions, lookupFrom, spookyBroad)

    if (flattenPagesPattern != null) result.flattenPages(flattenPagesPattern,flattenPagesIndexKey)
    else result
  }

  private def _fetch(
                      traces: Set[Trace],
                      joinType: JoinType,
                      numPartitions: Int,
                      lookupFrom: RDD[PageLike],
                      spookyBroad: Broadcast[SpookyContext]
                      ): PageRowRDD = {

    def _spooky: SpookyContext = spookyBroad.value

    import org.apache.spark.SparkContext._

    val _trace = traces.autoSnapshot

    val traceToRow = this.flatMap {
      row =>
        _trace.interpolate(row).map(interpolatedTrace => interpolatedTrace -> row)
    }

    val traceDistinct = traceToRow.map(_._1).distinct(numPartitions)

    val traceToPages = if (lookupFrom == null) {
      traceDistinct.map{
        trace => trace -> trace.resolve(_spooky)
      }
    }
    else {
      //----------------lookup start-------------------
      val lookupBacktraceToPages = lookupFrom //key unique due to groupBy
        .keyBy(_.uid)
        .reduceByKey((v1,v2) => PageUtils.laterOf(v1,v2))
        .values
        .groupBy(_.uid.backtrace)
        .mapValues{
        pages =>
          val _pages = pages.toSeq
          val sorted = _pages.sortBy(_.uid.blockIndex)
          sorted.slice(0, sorted.head.uid.total)
      }

      val backtraceToTraceIndex = traceDistinct.flatMap{ //key unique: traceDistinct.dryrun still yield distinct trace
        trace =>
          val dryruns = trace.dryrun.zipWithIndex
          if (dryruns.nonEmpty) dryruns.map(tuple => tuple._1 -> (trace, tuple._2))
          else Seq(null.asInstanceOf[Trace] -> (trace, 0))
      }

      val joinedTraceIndexToPages = backtraceToTraceIndex.leftOuterJoin(lookupBacktraceToPages)
      joinedTraceIndexToPages.map{
        tuple =>
          val backtrace = tuple._1
          val traceIndexToPages = tuple._2
          traceIndexToPages._2.foreach{
            seq =>
              seq.foreach{
                page =>
                  val pageBacktrace = page.uid.backtrace
                  pageBacktrace.inject(backtrace.asInstanceOf[pageBacktrace.type])
              }
          }
          traceIndexToPages._1._1 -> (traceIndexToPages._1._2, traceIndexToPages._2)
      }.groupByKey(numPartitions).map{
        tuple =>
          if (tuple._2.map(_._2).toSeq.contains(None)) tuple._1 -> tuple._1.resolve(_spooky)
          else {
            val pages = tuple._2.toSeq.sortBy(_._1).flatMap(_._2.get)
            tuple._1 -> pages
          }
      }
      //----------------lookup finish-------------------
    }

    val joinedRowToPages = traceToRow.leftOuterJoin(traceToPages, numPartitions = numPartitions)
    val RowToPages = joinedRowToPages.map(_._2)
    this.copy(self = RowToPages.flatMap(tuple => tuple._1.putPages(tuple._2.get, joinType)))

    //    val withTraceSquashed = withTrace.groupByKey()
    //    val withPagesSquashed = withTraceSquashed.map{ //Unfortunately there is no mapKey
    //      tuple => tuple._1.resolve(_spooky) -> tuple._2
    //    }
    //    val withPages = withPagesSquashed.flatMapValues(rows => rows).map(identity)
    //    val result = this.copy(
    //      self = withPages.flatMap(tuple => tuple._2.putPages(tuple._1, joinType))
    //    )
  }

  def join(
            expr: Expression[Any], //name is discarded
            indexKey: Symbol = null, //left & idempotent parameters are missing as they are always set to true
            limit: Int = spooky.joinLimit
            )(
            traces: Set[Trace],
            joinType: JoinType = Const.defaultJoinType,
            numPartitions: Int = this.sparkContext.defaultParallelism,
            flattenPagesPattern: Symbol = '*,
            flattenPagesIndexKey: Symbol = null
            )(
            select: Expression[Any]*
            ): PageRowRDD = {

    this
      .flattenTemp(expr defaultAs Symbol(Const.defaultJoinKey), indexKey, limit, left = true)
      .fetch(traces, joinType, numPartitions, flattenPagesPattern, flattenPagesIndexKey)
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
                 indexKey: Symbol = null, //left & idempotent parameters are missing as they are always set to true
                 limit: Int = spooky.joinLimit,
                 joinType: JoinType = Const.defaultJoinType,
                 numPartitions: Int = this.sparkContext.defaultParallelism,
                 select: Expression[Any] = null
                 ): PageRowRDD =
    this.join(expr, indexKey, limit)(
      Visit(new GetExpr(Const.defaultJoinKey)),
      joinType,
      numPartitions
    )(Option(select).toSeq: _*)

  /**
   * same as join, but avoid launching a browser by using direct http GET (wget) to download new pages
   * much faster and less stressful to both crawling and target server(s)
   * @param limit only the first n links will be used, default to Const.fetchLimit
   * @return RDD[Page]
   */
  def wgetJoin(
                expr: Expression[Any],
                indexKey: Symbol = null, //left & idempotent parameters are missing as they are always set to true
                limit: Int = spooky.joinLimit,
                joinType: JoinType = Const.defaultJoinType,
                numPartitions: Int = this.sparkContext.defaultParallelism,
                select: Expression[Any] = null
                ): PageRowRDD =
    this.join(expr, indexKey, limit)(
      Wget(new GetExpr(Const.defaultJoinKey)),
      joinType,
      numPartitions
    )(Option(select).toSeq: _*)

  def distinctSignature(
                         ignore: Iterable[Symbol],
                         numPartitions: Int = this.sparkContext.defaultParallelism
                         ): PageRowRDD = {
    import org.apache.spark.SparkContext._

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

    import org.apache.spark.SparkContext._

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

  //recursive join and union! applicable to many situations like (wide) pagination and deep crawling
  //TODO: secondary engine allowing 'narrow' asynchronous explore
  def explore(
               expr: Expression[Any],
               depthKey: Symbol = null,
               indexKey: Symbol = null, //left & idempotent parameters are missing as they are always set to true
               maxDepth: Int = spooky.maxExploreDepth
               )(
               traces: Set[Trace],
               numPartitions: Int = this.sparkContext.defaultParallelism,
               flattenPagesPattern: Symbol = '*,
               flattenPagesIndexKey: Symbol = null
               )(
               select: Expression[Any]*
               ): PageRowRDD = {

    var newRows = this

    var total = if (depthKey != null)
      this.select(Literal(0) ~ depthKey)
        .copy(indexKeys = this.indexKeys + Key(depthKey))
    else this

    var totalPages =this.flatMap(_.pageLikes)

    val spookyBroad = this.context.broadcast(this.spooky)
    if (this.context.getCheckpointDir.isEmpty) this.context.setCheckpointDir(spooky.dir.checkpoint)

    for (depth <- 1 to maxDepth) {
      //always inner join
      val joinedBeforeFlatten = newRows
        .flattenTemp(expr defaultAs Symbol(Const.defaultJoinKey), indexKey, Int.MaxValue, left = true)
        ._fetch(traces, Inner, numPartitions, totalPages, spookyBroad)

      val joinedPages = joinedBeforeFlatten.flatMap(_.pageLikes)

      import org.apache.spark.SparkContext._

      totalPages = totalPages.union(joinedPages)
        .keyBy(_.uid)
        .reduceByKey((v1,v2) => PageUtils.laterOf(v1,v2), numPartitions)
        .values
      if (depth % 20 == 0) {
        totalPages.persist().checkpoint()
        val size = totalPages.count()
        LoggerFactory.getLogger(this.getClass).info(s"explored $size pages in total")
      }

      val joined = joinedBeforeFlatten.flattenPages(flattenPagesPattern, flattenPagesIndexKey)

      val allIgnoredKeys = Seq(depthKey, indexKey, flattenPagesIndexKey).filter(_ != null)

      newRows = joined
        .subtractSignature(total, allIgnoredKeys)
        .distinctSignature(allIgnoredKeys)
        .persist()

      if (depth % 20 == 0) {
        newRows.checkpoint()
      }

      val newRowsSize = newRows.count()
      LoggerFactory.getLogger(this.getClass).info(s"fetched $newRowsSize new row(s)")

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
      if (depth % 20 == 0) {
        total.persist().checkpoint()
        val size = total.count()
        LoggerFactory.getLogger(this.getClass).info(s"fetched $size rows in total")
      }
    }

    total
      .select(select: _*)
      .clearTemp
      .coalesce(numPartitions)
  }

  def visitExplore(
                    expr: Expression[Any],
                    depthKey: Symbol = null,
                    indexKey: Symbol = null, //left & idempotent parameters are missing as they are always set to true
                    maxDepth: Int = spooky.maxExploreDepth,
                    numPartitions: Int = this.sparkContext.defaultParallelism,
                    select: Expression[Any] = null
                    ): PageRowRDD =
    explore(expr, depthKey, indexKey, maxDepth)(
      Visit(new GetExpr(Const.defaultJoinKey)),
      numPartitions
    )(Option(select).toSeq: _*)

  def wgetExplore(
                   expr: Expression[Any],
                   depthKey: Symbol = null,
                   indexKey: Symbol = null, //left & idempotent parameters are missing as they are always set to true
                   maxDepth: Int = spooky.maxExploreDepth,
                   numPartitions: Int = this.sparkContext.defaultParallelism,
                   select: Expression[Any] = null
                   ): PageRowRDD =
    explore(expr, depthKey, indexKey, maxDepth)(
      Wget(new GetExpr(Const.defaultJoinKey)),
      numPartitions
    )(Option(select).toSeq: _*)

  /**
   * insert many pages for each old page by recursively visiting "next page" link
   * @param selector selector of the "next page" element
   * @param limit depth of recursion
   * @return RDD[Page], contains both old and new pages
   *         TODO: deprecate
   */
  def paginate(
                selector: String,
                attr: String = "abs:href",
                wget: Boolean = true,
                postAction: Seq[Action] = Seq()
                )(
                limit: Int = spooky.paginationLimit,
                indexKey: Symbol = null,
                flatten: Boolean = true,
                last: Boolean = false
                ): PageRowRDD = {

    val spookyBroad = this.context.broadcast(this.spooky)
    def _spooky: SpookyContext = spookyBroad.value

    val realIndexKey = Key(indexKey)

    val result = this.flatMap {
      _.paginate(selector, attr, wget, postAction)(limit, Key(indexKey), flatten)(_spooky)
    }

    this.copy(
      self = result,
      keys = this.keys ++ Option(realIndexKey),
      indexKeys = this.indexKeys ++ Option(Key(indexKey))
    )
  }
}