package org.tribbloid.spookystuff.sparkbinding

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.{Partition, TaskContext}
import org.tribbloid.spookystuff.actions._
import org.tribbloid.spookystuff.dsl.{Inner, JoinType, _}
import org.tribbloid.spookystuff.entity._
import org.tribbloid.spookystuff.expressions._
import org.tribbloid.spookystuff.pages.Page
import org.tribbloid.spookystuff.utils._
import org.tribbloid.spookystuff.{Const, SpookyContext}

import scala.collection.immutable.ListSet
import scala.language.implicitConversions
import scala.reflect.ClassTag

/**
 * Created by peng on 8/29/14.
 */
case class PageRowRDD(
                       @transient self: RDD[PageRow],
                       @transient keys: ListSet[KeyLike] = ListSet(),
                       @transient spooky: SpookyContext
                       )
  extends RDD[PageRow](self) {

  @DeveloperApi
  override def compute(split: Partition, context: TaskContext): Iterator[PageRow] =
    firstParent[PageRow].compute(split, context).map(_.copy())

  override protected def getPartitions: Array[Partition] = firstParent[PageRow].partitions

  //-----------------------------------------------------------------------

  private implicit def selfToPageRowRDD(self: RDD[PageRow]): PageRowRDD = this.copy(self = self)

  override def filter(f: PageRow => Boolean): PageRowRDD = super.filter(f)

  override def distinct(): PageRowRDD =
    super.distinct()

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
        this.keys ++ other.keys
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
        this.keys -- other.keys
      )
    case _ => super.intersection(other)
  }

  override def intersection(other: RDD[PageRow], numPartitions: Int) = other match {

    case other: PageRowRDD =>
      this.copy(
        super.intersection(other.self),
        this.keys -- other.keys
      )
    case _ => super.intersection(other, numPartitions)
  }
  //-------------------all before this lines are self typed wrappers--------------------

  def toPageRowRDD: PageRowRDD = this

  def toMapRDD: RDD[Map[String, Any]] = self.map(_.asMap())

  def toJsonRDD: RDD[String] = self.map(_.asJson())

  //TODO: use the new applySchema api to avoid losing type info
  def asSchemaRDD(): SchemaRDD = {

    val jsonRDD = this.toJsonRDD

    jsonRDD.persist() //for some unknown reason SQLContext.jsonRDD uses the parameter RDD twice, this has to be fixed by somebody else

    //by default, will order the columns to be identical to the sequence they are extracted, data input will be ignored

    //TODO: handle missing columns
    this.spooky.sqlContext.jsonRDD(jsonRDD)
      .select(
        keys.toSeq.filter(_.isInstanceOf[Key]).reverse.map(key => UnresolvedAttribute(Utils.canonizeColumnName(key.name))): _*
      )
  }

  def asCsvRDD(separator: String = ","): RDD[String] = this.asSchemaRDD().map {
    _.mkString(separator)
  }

  def asTsvRDD(): RDD[String] = this.asCsvRDD("\t")

  //  def groupByData(): PageSchemaRDD = {
  //    import org.apache.spark.SparkContext._
  //
  //    val result = self.map(row => (row.cells, row.pages))
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
  //TODO: obsolete
  def saveContent(
                   select: PageRow => Any = null,
                   extract: Page => Any = null,
                   overwrite: Boolean = false
                   ): PageRowRDD = {
    assert(select != null || extract != null)

    val spookyBroad = self.context.broadcast(this.spooky)

    val result = self.map {

      pageRow => {

        val selectPath = select match {
          case f: (PageRow => Any) => f(pageRow).toString
          case _ => ""
        }

        val newPages = pageRow.pages.map {
          page => {

            val extractPath = extract match {
              case f: (Page => Any) => f(page).toString
              case _ => ""
            }

            val path = Utils.urlConcat(selectPath, extractPath)

            page.save(
              Seq(path),
              overwrite = overwrite
            )(spookyBroad.value)
          }
        }

        pageRow.copy(pages = newPages)
      }
    }

    this.copy(result)
  }

  /**
   * same as saveAs
   * but this is an action that will be executed immediately
   * @param overwrite if a file with the same name already exist:
   *                  true: overwrite it
   *                  false: append an unique suffix to the new file name
   * @return an array of file paths
   */
  def dumpContent(
                   select: PageRow => String = null,
                   extract: Page => String = null,
                   overwrite: Boolean = false
                   ): Array[String] = this.saveContent(select, extract, overwrite).self.flatMap {
    _.pages.map {
      _.saved
    }
  }.collect()

  //  /**
  //   * extract parts of each Page and insert into their respective context
  //   * if a key already exist in old context it will be replaced with the new one.
  //   * @param keyAndF a key-function map, each element is used to generate a key-value map using the Page itself
  //   * @return new RDD[Page]
  //   */
  //  def extract(keyAndF: (String, Page => Any)*): PageRowRDD = {
  //    val keys = this.keys ++ keyAndF.map(tuple => Key(tuple._1))
  //
  //    this.copy(
  //      self = self.map(_.extract(keyAndF)),
  //      keys = keys
  //    )
  //  }

  def select(exprs: Expr[Any]*): PageRowRDD = {

    //    val _exprs = exprs.filterNot {
    //      case ex: GetExpr => ex.name == ex.name
    //      case _ => false
    //    }
    val _exprs = exprs

    val newKeys: Seq[Key] = _exprs.map {
      expr =>
        val key = Key(expr.name)
        assert(!this.keys.contains(key)) //can't insert the same key twice
        key
    }

    val result = this.copy(
      self = self.map(_.select(_exprs)),
      keys = this.keys ++ newKeys
    )
    result
  }

  private def selectTemp(exprs: Expr[Any]*): PageRowRDD = {

    //    val _exprs = exprs.filterNot {
    //      case ex: GetExpr => ex.name == ex.name
    //      case _ => false
    //    } //TODO: optional!
    val _exprs = exprs

    val newKeys: Seq[TempKey] = _exprs.map {
      expr =>
        val key = TempKey(expr.name)
        key
    }

    this.copy(
      self = self.map(_.selectTemp(_exprs)),
      keys = this.keys ++ newKeys
    )
  }

  def remove(keys: Symbol*): PageRowRDD = {
    val names = keys.map(key => Key(key))
    this.copy(
      self = self.map(_.remove(names)),
      keys = this.keys -- names
    )
  }

  //  private def clearTemp: PageRowRDD = {
  //    this.copy(
  //      self = self.map(_.filterKeys(!_.isInstanceOf[TempKey])),
  //      keys = keys -- keys.filter(_.isInstanceOf[TempKey])//circumvent https://issues.scala-lang.org/browse/SI-8985
  //    )
  //  }

  def flatten(
               expr: Expr[Any],
               indexKey: Symbol = null,
               limit: Int = Int.MaxValue,
               left: Boolean = true
               ): PageRowRDD = {
    val selected = this.select(expr)

    val flattened = selected.self.flatMap(_.flatten(expr.name, Key(indexKey), limit, left))
    selected.copy(
      self = flattened,
      keys = selected.keys ++ Option(Key(indexKey))
    )
  }

  private def flattenTemp(
                           expr: Expr[Any],
                           indexKey: Symbol = null,
                           limit: Int = Int.MaxValue,
                           left: Boolean = true
                           ): PageRowRDD = {
    val selected = this.selectTemp(expr)

    val flattened = selected.self.flatMap(_.flatten(expr.name, Key(indexKey), limit, left))
    selected.copy(
      self = flattened,
      keys = selected.keys ++ Option(Key(indexKey))
    )
  }

  def explode(
               expr: Expr[Any],
               indexKey: Symbol = null,
               limit: Int = Int.MaxValue,
               left: Boolean = true
               ): PageRowRDD = flatten(expr, indexKey, limit, left)

  def flatSelect(
                  expr: Expr[Any],
                  indexKey: Symbol = null,
                  limit: Int = Int.MaxValue,
                  left: Boolean = true
                  )(exprs: Expr[Any]*) ={

    this
      .flattenTemp(expr defaultAs Symbol(Const.joinExprKey), indexKey, limit, left)
      .select(exprs: _*)
    //      .clearTemp
  }

  def flattenPages(
                    pattern: Symbol = '*,
                    indexKey: Symbol = null
                    ): PageRowRDD =
    this.copy(
      self = self.flatMap(_.flattenPages(pattern.name, Key(indexKey))),
      keys = this.keys ++ Option(Key(indexKey))
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
                 numPartitions: Int = self.sparkContext.defaultParallelism,
                 flattenPagesPattern: Symbol = '*, //by default, always flatten all pages
                 flattenPagesIndexKey: Symbol = null
                 ): PageRowRDD = {

    val _trace = traces.autoSnapshot
    val withTrace = self.flatMap(
      row => _trace.interpolate(row).map(_ -> row)
    )

    val spookyBroad = self.context.broadcast(this.spooky)
    implicit def _spooky: SpookyContext = spookyBroad.value

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
             numPartitions: Int = self.sparkContext.defaultParallelism,
             flattenPagesPattern: Symbol = '*, //by default, always flatten all pages
             flattenPagesIndexKey: Symbol = null
             //TODO:             cache: RDD[Page] = null  & always use self as cache
             ): PageRowRDD = {

    import org.apache.spark.SparkContext._

    val _trace = traces.autoSnapshot
    val withTrace = self.flatMap(
      row => _trace.interpolate(row).map(_ -> row)
    )

    val spookyBroad = self.context.broadcast(this.spooky)
    implicit def _spooky: SpookyContext = spookyBroad.value

    //    val withTracePersisted = withTrace.persist()
    //    var traceDistinct = withTracePersisted.map(_._1).distinct(numPartitions)
    //    val traceWithPages = traceDistinct.map(trace => trace -> trace.resolve(_spooky))
    //    val withPages = withTracePersisted.leftOuterJoin(traceWithPages, numPartitions = numPartitions).map(_._2)
    //    val result = this.copy(self = withPages.flatMap(tuple => tuple._1.putPages(tuple._2.get, joinType)))

    val withTraceSquashed = withTrace.groupByKey()
    val withPagesSquashed = withTraceSquashed.map{ //Unfortunately there is no mapKey
      tuple => tuple._1.resolve(_spooky) -> tuple._2
    }
    val withPages = withPagesSquashed.flatMapValues(rows => rows).map(identity)
    val result = this.copy(
      self = withPages.flatMap(tuple => tuple._2.putPages(tuple._1, joinType))
    )

    if (flattenPagesPattern != null) result.flattenPages(flattenPagesPattern,flattenPagesIndexKey)
    else result
  }

  def join(
            expr: Expr[Any], //name is discarded
            indexKey: Symbol = null, //left & idempotent parameters are missing as they are always set to true
            limit: Int = spooky.joinLimit
            )(
            traces: Set[Trace],
            joinType: JoinType = Const.defaultJoinType,
            numPartitions: Int = self.sparkContext.defaultParallelism,
            flattenPagesPattern: Symbol = '*,
            flattenPagesIndexKey: Symbol = null
            )(
            exprs: Expr[Any]*
            ): PageRowRDD = {

    this
      .flattenTemp(expr defaultAs Symbol(Const.joinExprKey), indexKey, limit, left = true)
      .fetch(traces, joinType, numPartitions, flattenPagesPattern, flattenPagesIndexKey)
      .select(exprs: _*)
    //      .clearTemp
  }

  /**
   * results in a new set of Pages by crawling links on old pages
   * old pages that doesn't contain the link will be ignored
   * @param limit only the first n links will be used, default to Const.fetchLimit
   * @return RDD[Page]
   */
  def visitJoin(
                 expr: Expr[Any],
                 indexKey: Symbol = null, //left & idempotent parameters are missing as they are always set to true
                 limit: Int = spooky.joinLimit
                 )(
                 joinType: JoinType = Const.defaultJoinType,
                 numPartitions: Int = self.sparkContext.defaultParallelism
                 ): PageRowRDD =
    this.join(expr, indexKey, limit)(Visit(new GetExpr(Const.joinExprKey)), joinType, numPartitions)()

  /**
   * same as join, but avoid launching a browser by using direct http GET (wget) to download new pages
   * much faster and less stressful to both crawling and target server(s)
   * @param limit only the first n links will be used, default to Const.fetchLimit
   * @return RDD[Page]
   */
  def wgetJoin(
                expr: Expr[Any],
                indexKey: Symbol = null, //left & idempotent parameters are missing as they are always set to true
                limit: Int = spooky.joinLimit
                )(
                joinType: JoinType = Const.defaultJoinType,
                numPartitions: Int = self.sparkContext.defaultParallelism
                ): PageRowRDD =
    this.join(expr, indexKey, limit)(Wget(new GetExpr(Const.joinExprKey)), joinType, numPartitions)()

  def distinctSignature(
                         exclude: Iterable[Symbol],
                         numPartitions: Int = self.sparkContext.defaultParallelism
                         ): PageRowRDD = {
    import org.apache.spark.SparkContext._

    val excludeKeys = exclude.map(Key(_))
    val selfWithSignatures = self.map{
      row => row.signature(excludeKeys) -> row
    }
    val distinct = selfWithSignatures.reduceByKey((row1, row2) => row1).values

    this.copy(
      self = distinct
    )
  }

  /* if the following conditions are met, row will be removed
* 1. row has identical trace and name
* 2. the row has identical cells*/
  def subtractSignature(
                         others: RDD[PageRow],
                         exclude: Iterable[Symbol],
                         numPartitions: Int = self.sparkContext.defaultParallelism
                         ): PageRowRDD = {

    import org.apache.spark.SparkContext._

    val excludeKeys = exclude.map(Key(_))
    val otherSignatures = others.map {
      row => row.signature(excludeKeys) -> null
    }
    val selfWithSignatures = self.map{
      row => row.signature(excludeKeys) -> row
    }
    val excluded = selfWithSignatures.subtractByKey(otherSignatures, numPartitions).values

    this.copy(
      self = excluded
    )
  }

  //TODO: handling of exprs is defective!
  //recursive join and union! applicable to many situations like (wide) pagination and deep crawling
  def explore(
               expr: Expr[Any],
               indexKey: Symbol = null, //left & idempotent parameters are missing as they are always set to true
               maxDepth: Int = spooky.maxExploreDepth
               )(
               traces: Set[Trace],
               numPartitions: Int = self.sparkContext.defaultParallelism,
               flattenPagesPattern: Symbol = '*,
               flattenPagesIndexKey: Symbol = null,
               depthKey: Symbol = null
               )(
               exprs: Expr[Any]*
               ): PageRowRDD = {

    var newRows = this
    var total = if (depthKey != null) this.select(Literal(0) > depthKey).self
    else this.self

    for (depth <- 1 to maxDepth) {
      //always inner join
      val joined = newRows
        .flattenTemp(expr defaultAs Symbol(Const.joinExprKey), indexKey, Int.MaxValue, left = true)
        .fetch(traces, Inner, numPartitions, flattenPagesPattern, flattenPagesIndexKey)
        .select(exprs: _*)
      //        .clearTemp

      newRows = joined.distinctSignature(Option(depthKey)).subtractSignature(total, Option(depthKey)).persist()

      val newRowsCount = newRows.count()

      if (newRowsCount == 0){
        return this.copy(self = total.coalesce(numPartitions)) //early stop condition if no new pages with the same data is detected
      }

      total = total.union(
        if (depthKey != null) newRows.select(new Literal(depth) > depthKey).self
        else newRows.self
      )
    }

    this.copy(self = total.coalesce(numPartitions))
  }

  def visitExplore(
                    expr: Expr[Any],
                    indexKey: Symbol = null, //left & idempotent parameters are missing as they are always set to true
                    maxDepth: Int = spooky.maxExploreDepth
                    )(
                    numPartitions: Int = self.sparkContext.defaultParallelism,
                    depthKey: Symbol = null
                    ): PageRowRDD = explore(expr, indexKey, maxDepth)(Visit(new GetExpr(Const.joinExprKey)), numPartitions, depthKey = depthKey)()

  def wgetExplore(
                   expr: Expr[Any],
                   indexKey: Symbol = null, //left & idempotent parameters are missing as they are always set to true
                   maxDepth: Int = spooky.maxExploreDepth
                   )(
                   numPartitions: Int = self.sparkContext.defaultParallelism,
                   depthKey: Symbol = null
                   ): PageRowRDD = explore(expr, indexKey, maxDepth)(Wget(new GetExpr(Const.joinExprKey)), numPartitions, depthKey = depthKey)()

  //  /**
  //   * break each page into 'shards', used to extract structured data from tables
  //   * @param selector denotes enclosing elements of each shards
  //   * @param limit only the first n elements will be used, default to Const.fetchLimit
  //   * @return RDD[Page], each page will generate several shards
  //   */
  //  def sliceJoin(
  //                 selector: String,
  //                 expand: Int = 0
  //                 )(
  //                 limit: Int = spooky.sliceLimit, //applied after distinct
  //                 indexKey: Symbol = null,
  //                 joinType: JoinType = Const.defaultJoinType,
  //                 flatten: Boolean = true
  //                 ): PageRowRDD = {
  //
  //    val _indexKey = Key(indexKey)
  //
  //    val result = self.flatMap {
  //      _.slice(selector, expand)(limit, _indexKey, joinType, flatten)
  //    }
  //
  //    this.copy(result, this.keys ++ Option(_indexKey))
  //  }

  /**
   * insert many pages for each old page by recursively visiting "next page" link
   * @param selector selector of the "next page" element
   * @param limit depth of recursion
   * @return RDD[Page], contains both old and new pages
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

    val spookyBroad = self.context.broadcast(this.spooky)

    implicit def spookyImplicit: SpookyContext = spookyBroad.value

    val realIndexKey = Key(indexKey)

    val result = self.flatMap {
      _.paginate(selector, attr, wget, postAction)(limit, Key(indexKey), flatten)(spooky)
    }

    this.copy(result, this.keys ++ Option(realIndexKey))
  }
}