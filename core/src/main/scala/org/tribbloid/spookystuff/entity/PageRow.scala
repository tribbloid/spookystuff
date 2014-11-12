package org.tribbloid.spookystuff.entity

import org.tribbloid.spookystuff.SpookyContext
import org.tribbloid.spookystuff.actions._
import org.tribbloid.spookystuff.expressions._
import org.tribbloid.spookystuff.utils._

/**
 * Created by peng on 8/29/14.
 */
//some guideline: All key parameters are Symbols to align with Spark SQL.
//cells & pages share the same key pool but different data structure
case class PageRow(
                    cells: Map[KeyLike, Any] = Map(), //TODO: also carry PageUID & property type (Vertex/Edge)
                    pages: Seq[Page] = Seq() // discarded after new page coming in
                    )
  extends Serializable {

  //TempKey precedes ordinary Key because they are ephemeral
  def get(keyStr: String): Option[Any] = {
    if (cells.contains(TempKey(keyStr))) cells.get(TempKey(keyStr))
    else cells.get(Key(keyStr))
  }

  def asMap(): Map[String, Any] = this.cells.filterKeys(!_.isInstanceOf[TempKey]).map( tuple => tuple._1.name -> tuple._2)

  def asJson(): String = Utils.toJson(this.asMap())

  //retain old pageRow,
  //always left
  def flatten(
               keyStr: String,
               indexKey: Key,
               limit: Int,
               left: Boolean
               ): Seq[PageRow] = {

    val tempKey = TempKey(keyStr)
    val key = if (cells.contains(tempKey)) tempKey
    else Key(keyStr)

    val newCells =cells.flattenKey(key, indexKey).slice(0, limit)
    val result = newCells.map(cell => this.copy(cells = cell))
    if (left && result.isEmpty) Seq(this) //this will make sure you dont't lose anything
    else result
  }

  //always left, discard old page row
  def flattenPages(
                    pattern: String, //TODO: enable soon
                    indexKey: Key
                    ): Seq[PageRow] = {
    val result = if (indexKey == null) {
      this.pages.map{
        page => this.copy(cells = this.cells, pages = Seq(page))
      }
    }
    else {
      this.pages.zipWithIndex.map{
        tuple => this.copy(cells = this.cells + (indexKey -> tuple._2), pages = Seq(tuple._1))
      }
    }

    if (result.isEmpty) {
      Seq(this.copy(pages = Seq()))
    }
    else {
      result
    }
  }

  def putPages(others: Seq[Page], joinType: JoinType): Option[PageRow] = {
    joinType match {
      case Inner =>
        if (others.isEmpty) None
        else Some(this.copy(pages = others))
      case LeftOuter => Some(this.copy(pages = others))
      case Replace =>
        if (others.isEmpty) Some(this)
        else Some(this.copy(pages = this.pages ++ others))
      case Append =>
        Some(this.copy(pages = this.pages ++ others))
      case Merge =>
        val oldUids = this.pages.map(_.uid)
        val newPages = others.filter(newPage => !oldUids.contains(newPage.uid))
        Some(this.copy(pages = this.pages ++ newPages))
    }
  }

  //only apply to last page
  //TODO: don't use any String that contains dot as column name, or you will encounter bug SPARK-2775
  //see https://issues.apache.org/jira/browse/SPARK-2775
  def extract(keyAndF: Seq[(String, Page => Any)]): PageRow = {

    this.pages.lastOption match {
      case None => this
      case Some(page) =>
        val map = Map(
          keyAndF.map{
            tuple => (new Key(tuple._1), tuple._2(page))
          }: _*
        )

        this.copy(cells = this.cells ++ map)
    }
  }

  //TODO: don't use any String that contains dot as column name, or you will encounter bug SPARK-2775
  //TODO: this will become the default extract at some point, but not now
  def select(keys: Seq[KeyLike], fs: Seq[PageRow => _]): PageRow = {

    val newKVs = Map(
      keys.zip(fs).map{
        tuple =>
          assert(!this.cells.contains(tuple._1))
          tuple._1 -> tuple._2(this)
      }: _*
    )
    this.copy(cells = this.cells ++ newKVs)
  }

  def remove(keys: Seq[KeyLike]): PageRow = {
    this.copy(cells = this.cells -- keys)
  }

  def filterKeys(f: KeyLike => Boolean): PageRow = {
    this.copy(cells = this.cells.filterKeys(f))
  }

  //affect last page
  def slice(
             selector: String,
             expand: Int
             )(
             limit: Int, //applied after distinct
             indexKey: Key,
             joinType: JoinType,
             flatten: Boolean
             ): Seq[PageRow] = {

    val sliced = this.pages.lastOption match {
      case None => Seq[Page]()
      case Some(page) => page.slice(selector, expand)(limit)
    }

    val results = this.putPages(sliced, joinType).toSeq

    if (flatten) results.flatMap(_.flattenPages("*", indexKey = indexKey))
    else results
  }

  //affect last page
  //TODO: switch to recursive !>< to enable parallelization
  //TODO: lambda support
  def paginate(
                selector: String,
                attr: String,
                wget: Boolean,
                postActions: Seq[Action]
                )(
                limit: Int,
                indexKey: Key,
                flatten: Boolean,
                last: Boolean
                )(
                implicit spooky: SpookyContext
                ): Seq[PageRow] = {

    var currentRow = this
    var increment = this.pages.size

    while (currentRow.pages.size <= limit && increment > 0 && currentRow.pages.last.attrExist(selector, attr)) {

      val action = if (!wget) Visit(currentRow.pages.last.attr1(selector, attr, noEmpty = true, last = last))
      else Wget(currentRow.pages.last.attr1(selector, attr, noEmpty = true, last = last))

      val newRow = currentRow.putPages(Trace(action::Nil).resolve(spooky), joinType = Merge).get

      increment = newRow.pages.size - currentRow.pages.size

      currentRow = newRow
    }

    if (flatten) currentRow.flattenPages("*",indexKey = indexKey)
    else Seq(currentRow)
  }
}