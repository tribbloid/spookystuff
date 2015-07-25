package org.tribbloid.spookystuff.entity

import org.apache.spark.rdd.RDD
import org.slf4j.LoggerFactory
import org.tribbloid.spookystuff.actions._
import org.tribbloid.spookystuff.dsl._
import org.tribbloid.spookystuff.entity.PageRow.{RowUID, KVStore, SegID}
import org.tribbloid.spookystuff.expressions._
import org.tribbloid.spookystuff.pages._
import org.tribbloid.spookystuff.utils._
import org.tribbloid.spookystuff.{Const, SpookyContext}

import scala.collection.immutable.ListMap
import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag
import scala.util.Random

/**
 * Created by peng on 8/29/14.
 * cells & pages share the same key pool but different data structure
 */
case class PageRow(
                    //ListMap may be slower but has tighter serialization size
                    store: KVStore = ListMap(), //TODO: also carry PageUID & property type (Vertex/Edge) for GraphX
                    pageLikes: Array[PageLike] = Array(), // discarded after new page coming in
                    segmentID: SegID = Random.nextLong() //keep flattened rows together //unique for an entire context.
                    ) {

  def pages: Array[Page] = pageLikes.flatMap {
    case page: Page => Some(page)
    case _ => None
  }

  def noPages: Array[NoPage] = pageLikes.flatMap {
    case noPage: NoPage => Some(noPage)
    case _ => None
  }

  def uid: RowUID = pageLikes.toSeq.map(_.uid) -> segmentID

  private def resolveKey(keyStr: String): KeyLike = {
    val tempKey = TempKey(keyStr)
    if (store.contains(tempKey)) tempKey
    else Key(keyStr)
  }

  def getInt(keyStr: String): Option[Int] = {
    this.get(keyStr).flatMap {
      case v: Int => Some(v)
      case _ => None
    }
  }

  def getIntIterable(keyStr: String): Option[Iterable[Int]] = {
    this.getTyped[Iterable[Int]](keyStr)
      .orElse{
      this.getInt(keyStr).map(Seq(_))
    }
  }

  //TempKey precedes ordinary Key
  //T cannot <: AnyVal otherwise will run into https://issues.scala-lang.org/browse/SI-6967
  def getTyped[T <: AnyRef: ClassTag](keyStr: String): Option[T] = {
    val res = this.get(keyStr).flatMap {
      case v: T => Some(v)
      case _ => None
    }
    res
  }

  //T cannot <: AnyVal otherwise will run into https://issues.scala-lang.org/browse/SI-6967
  def getTypedIterable[T <: AnyRef: ClassTag](keyStr: String): Option[Iterable[T]] = {
    val res1 = this.getTyped[Iterable[T]](keyStr)

    val res2 = res1.orElse{
      this.getTyped[T](keyStr).map(Seq(_))
    }
    res2
  }

  def get(keyStr: String): Option[Any] = get(resolveKey(keyStr))

  def get(key: KeyLike): Option[Any] = store.get(key)

  def getOnlyPage: Option[Page] = {
    val pages = this.pages

    if (pages.length > 1) throw new UnsupportedOperationException("Ambiguous key referring to multiple pages")
    else pages.headOption
  }

  def getPage(keyStr: String): Option[Page] = {

    if (keyStr == Const.onlyPageWildcard) return getOnlyPage

    val pages = this.pages.filter(_.name == keyStr)

    if (pages.length > 1) throw new UnsupportedOperationException("Ambiguous key referring to multiple pages")
    else pages.headOption
  }

  def getUnstructured(keyStr: String): Option[Unstructured] = {

    val page = getPage(keyStr)
    val value = get(keyStr).flatMap {
      case u: Unstructured => Option(u)
      case _ => None
    }

    if (page.nonEmpty && value.nonEmpty) throw new UnsupportedOperationException("Ambiguous key referring to both pages and data")
    else page.orElse(value)
  }

  //replace each '{key} in a string with their respective value in cells
  def replaceInto(
                   str: String,
                   delimiter: String = Const.keyDelimiter
                   ): Option[String] = {
    if (str == null) return None
    if (str.isEmpty) return Some(str)

    val regex = (delimiter+"\\{[^\\{\\}\r\n]*\\}").r

    val result = regex.replaceAllIn(str,m => {
      val original = m.group(0)
      val key = original.substring(2, original.length-1)
      this.get(key) match {
        case Some(v) => v.toString
        case None => return None
      }
    })

    Some(result)
  }

  @transient lazy val dryrun: DryRun = pageLikes.toSeq.map(_.uid.backtrace).distinct

  def ordinal(sortKeys: Seq[KeyLike]): Seq[Option[Iterable[Int]]] = {
    val result = sortKeys.map(key => this.getIntIterable(key.name))
    result
  }

  def toMap: Map[String, Any] = this.store
    .filterKeys(_.isInstanceOf[Key]).map(identity)
    .map( tuple => tuple._1.name -> tuple._2)

  def toJSON: String = {
    import org.tribbloid.spookystuff.views._

    Utils.toJson(this.toMap.canonizeKeysToColumnNames)
  }

  def select(exprs: Expression[Any]*): Option[PageRow] = _select(exprs, temp = false, clearExisting = false)

  def selectTemp(exprs: Expression[Any]*): Option[PageRow] = _select(exprs, temp = true, clearExisting = true)

  private def _select(
                       exprs: Seq[Expression[Any]],
                       temp: Boolean,
                       clearExisting: Boolean //set to true to ensure that repeated use of an alias (e.g. A for defaultJoinKey) always evict existing values to avoid data corruption.
                       ) = {
    val newKVs = exprs.map{
      expr =>
        val key = if (temp) TempKey(expr.name)
        else Key(expr.name)
        key -> expr(this)
    }

    val addKVs = newKVs.filter(_._2.nonEmpty).map(tuple => tuple._1 -> tuple._2.get)

    val existing = if (clearExisting) this.store -- newKVs.map(_._1)
    else this.store

    Some(this.copy(store = existing ++ addKVs)) //TODO: is there an negative impact of not removing empty KVs?
  }

  def remove(keys: KeyLike*): PageRow = {
    this.copy(store = this.store -- keys)
  }

  private def filterKeys(f: KeyLike => Boolean): PageRow = {
    this.copy(store = ListMap(this.store.filterKeys(f).toSeq: _*))
  }

  def clearTemp: PageRow = this.filterKeys(!_.isInstanceOf[TempKey])

  //for some join types (Inner/LeftOuter) existing Pages are always useless and should be discarded before shuffling
  def clearPagesBeforeFetch(joinType: JoinType): PageRow = {
    joinType match {
      case Inner=> this.copy(pageLikes = Array())
      case LeftOuter => this.copy(pageLikes = Array())
      case _ => this
    }
  }

  def putPages(others: Seq[PageLike], joinType: JoinType): Option[PageRow] = {
    joinType match {
      case Inner =>
        if (others.isEmpty) None
        else Some(this.copy(pageLikes = others.toArray))
      case LeftOuter =>
        Some(this.copy(pageLikes = others.toArray))
      case Replace =>
        if (others.isEmpty) Some(this)
        else Some(this.copy(pageLikes = others.toArray))
      case Append =>
        Some(this.copy(pageLikes = this.pageLikes ++ others))
      case Merge =>
        val oldUids = this.pageLikes.map(_.uid)
        val newPages = others.filter(newPage => !oldUids.contains(newPage.uid))
        Some(this.copy(pageLikes = this.pageLikes ++ newPages))
    }
  }

  //retain old pageRow,
  //always left
  def flatten(
               keyStr: String,
               ordinalKey: Symbol,
               maxOrdinal: Int,
               left: Boolean
               ): Seq[PageRow] = {

    val key = resolveKey(keyStr)

    import org.tribbloid.spookystuff.views._

    val newCells =store.flattenKey(key).slice(0, maxOrdinal)

    if (left && newCells.isEmpty) {
      Seq(this.copy(store = this.store - key)) //this will make sure you dont't lose anything
    }
    else {
      val result = newCells.map(newCell => this.copy(store = ListMap(newCell.toSeq: _*)))

      if (ordinalKey == null) result
      else result.zipWithIndex.flatMap{
        tuple =>
          tuple._1.select(Literal(tuple._2) ~+ ordinalKey) //multiple ordinalKey may be inserted sequentially in explore
      }
    }
  }

  //always left, discard old page row
  //warning: sometimes this always lose information regardless of pattern, e.g. all NoPage will be discarded
  def flattenPages(
                    pattern: String, //TODO: enable soon
                    ordinalKey: Symbol
                    ): Iterable[PageRow] = {

    //    val regex = pattern.r
    //
    //    val matches = regex.findAllIn(this.pages.map(_.name).mkString(","))
    //
    //    val flatNames = matches.map{
    //      mtch =>
    //        mtch.force
    //    }

    val contentRows = this.pages.map{
      page => this.copy(store = this.store, pageLikes = Array(page))
    }

    if (contentRows.isEmpty) {
      Iterable(this.copy(pageLikes = this.noPages.map(_.asInstanceOf[PageLike])))
    }
    else {

      val withOrdinalKey =
        if (ordinalKey == null) contentRows
        else contentRows.zipWithIndex.flatMap{
          tuple =>
            tuple._1.select(Literal(tuple._2) ~+ ordinalKey)
        }

      withOrdinalKey.zipWithIndex.map{
        tuple =>
          if (tuple._2 == 0) tuple._1.copy(pageLikes = tuple._1.pageLikes ++ this.noPages)
          else tuple._1
      }
    }
  }
  //yield a local explore stage that has smaller serialization size
  def localPreJoins(
                     joinExpr: Expression[Any],
                     ordinalKey: Symbol,
                     maxOrdinal: Int
                     )(
                     _traces: Set[Trace], //input of the explore to generate more pages from seeds
                     existingTraces: Set[Trace] = Set(Seq()),
                     existingDryruns: Set[DryRun] = Set(Seq())
                     ): Iterable[(Trace, PageRow)] = {

    this.selectTemp(joinExpr).toIterable
      .flatMap(_.flatten(joinExpr.name, ordinalKey, maxOrdinal, left = true)) //part of join locally
      .flatMap {
      //generate traces, still part of join locally
      row =>
        _traces.interpolate(row)
          .filterNot {
          //if trace or dryrun already exist returns None
          trace =>
            val traceExists = existingTraces.contains(trace) //if trace ...
          val dryrunExists = existingDryruns.contains(trace.dryrun) //... or dryrun exist
            traceExists || dryrunExists
        }
          .map(interpolatedTrace => interpolatedTrace -> row.clearPagesBeforeFetch(Inner))
    }
  }
}

/*
 used as:
 1. in-memory cache to quickly load pages from memory without reading disk or launching remote client
  (in this mode store is discarded immediately)
 2. sink for deep exploration.
 Lookup table is shared between all PageRowRDD from a SpookyContext, but deep exploration sink won't only be used locally.
 */
case class Squashed[T: ClassTag](
                                  pageLikes: Array[PageLike],
                                  metadata: Array[T] = Array() // data, segment, batchID to distinguish aggregated Rows from the same explore(), discarded upon consolidation
                                  ) {

  @transient lazy val dryrun: DryRun = pageLikes.toSeq.map(_.uid.backtrace).distinct

  def ++ (another: Squashed[T]): Squashed[T] = {
    this.copy(metadata = Array.concat[T](this.metadata, another.metadata))
  }
}

object PageRow {

  type KVStore =  ListMap[KeyLike, Any]

  type SegID = Long
  type RowUID = (Seq[PageUID], SegID)

  type WebCacheRow = (DryRun, Squashed[PageRow])

  type WebCacheRDD = RDD[WebCacheRow]

  def localExplore(
                    stage: ExploreStage,
                    spooky: SpookyContext
                    )(
                    joinExpr: Expression[Any],
                    depthKey: Symbol,
                    depthFromExclusive: Int,
                    depthToInclusive: Int,
                    ordinalKey: Symbol,
                    maxOrdinal: Int
                    )(
                    _traces: Set[Trace], //input of the explore to generate more pages from seeds
                    flattenPagesPattern: String,
                    flattenPagesOrdinalKey: Symbol
                    ): (Iterable[PageRow], ExploreStage) = { //PageRows that is one level deeper -> (new Seeds + existing traces/dryruns)

    val allNewRows: ArrayBuffer[PageRow] = ArrayBuffer()

    var trace_RowItr = stage.trace_RowItr.toIterable
    var existingTraces = stage.existingTraces

    for (depth <- depthFromExclusive + 1 to depthToInclusive) {

      //won't reduce too much
      val reducedRows: Map[Trace, Option[PageRow]] = trace_RowItr
        .groupBy(_._1)
        .map {
        tuple =>
          val firstOption = tuple._2.map(_._2).reduceOption(PageRow.reducer(ordinalKey))
          (tuple._1, firstOption)
        //when multiple links have identical uri/trace, keep the first one
      }

      existingTraces = existingTraces ++ reducedRows.keys

      val seeds = reducedRows
        .flatMap {
        reducedRow =>
          val newPages = reducedRow._1.resolve(spooky)
          val rows = reducedRow._2
          rows.flatMap(_.putPages(newPages, Inner))
      }
        .flatMap {
        row =>
          if (flattenPagesPattern != null) row.flattenPages(flattenPagesPattern, flattenPagesOrdinalKey)
          else Seq(row)
      }

      LoggerFactory.getLogger(this.getClass)
        .info(s"found ${seeds.size} new seed(s) after $depth iteration(s) [traces.size = ${existingTraces.size}, total.size = ${allNewRows.size}]")
      if (seeds.size == 0) return (allNewRows, stage.copy(trace_RowItr = Array(), existingTraces = existingTraces))

      trace_RowItr = seeds.flatMap{
        _.localPreJoins(joinExpr,ordinalKey,maxOrdinal)(
          _traces, existingTraces, stage.existingDryruns
        )
      }

      val newRows: Iterable[PageRow] = if (depthKey != null) seeds.flatMap(_.select(Literal(depth) ~ depthKey))
      else seeds

      allNewRows ++= newRows
    }

    (allNewRows, stage.copy(trace_RowItr = trace_RowItr.toArray, existingTraces = existingTraces))
  }

  //  def discoverLatestBatch(pages: Iterable[PageLike]): Option[Seq[PageLike]] = {
  //    //assume that all inputs already has identical backtraces
  //
  //    if (pages.isEmpty) return None
  //
  //    val blockIndexToPages = mutable.HashMap[Int, PageLike]()
  //    for (page <- pages) {
  //      val oldPage = blockIndexToPages.get(page.uid.blockIndex)
  //      if (oldPage.isEmpty || page.laterThan(oldPage.get)) blockIndexToPages.put(page.uid.blockIndex, page)
  //    }
  //    val sorted = blockIndexToPages.toSeq.sortBy(_._1).map(_._2)
  //
  //    //extensive sanity check to make sure that none of them are obsolete
  //    val total = sorted.head.uid.blockSize
  //    if (sorted.size < total) return None
  //    val trunk = sorted.slice(0, sorted.head.uid.blockSize)
  //
  //    trunk.foreach{
  //      page =>
  //        if (page.uid.blockSize != total) return None
  //    }
  //
  //    Some(sorted.slice(0, sorted.head.uid.blockSize))
  //  }

  def reducer(key: Symbol): (PageRow, PageRow) => PageRow = {
    (row1, row2) =>
      import Ordering.Implicits._

      if (key == null) { //TODO: This is a temporary solution, eventually hidden key will eliminate it
      val v1 = row1.pages.headOption.map(_.timestamp.getTime)
        val v2 = row2.pages.headOption.map(_.timestamp.getTime)
        if (v1 <= v2) row1
        else row2
      }
      else {
        val v1 = row1.getIntIterable(key.name)
        val v2 = row2.getIntIterable(key.name)
        if (v1 <= v2) row1
        else row2
      }
  }
}

//intermediate variable representing a stage in web crawling.
case class ExploreStage(
                         trace_RowItr: Array[(Trace, PageRow)], //pages that hasn't be been crawled before
                         existingTraces: Set[Trace] = Set(Seq()), //already resolved traces, Seq() is included as resolving it is pointless
                         existingDryruns: Set[DryRun] = Set(Seq()) //already resolved dryruns, Seq() is included as resolving it is pointless
                         ) {

  def hasMore = trace_RowItr.nonEmpty
}