package org.tribbloid.spookystuff.entity

import org.tribbloid.spookystuff.{views, SpookyContext}
import org.tribbloid.spookystuff.actions._
import org.tribbloid.spookystuff.dsl._
import org.tribbloid.spookystuff.expressions._
import org.tribbloid.spookystuff.pages.{Unstructured, Page}
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

  private def resolveKey(keyStr: String): KeyLike = {
    val tempKey = TempKey(keyStr)
    if (cells.contains(tempKey)) tempKey
    else Key(keyStr)
  }

  //TempKey precedes ordinary Key because they are ephemeral
  def get(keyStr: String): Option[Any] = {
    cells.get(resolveKey(keyStr))
  }

  def getPage(keyStr: String): Option[Page] = {

    val pages = if (keyStr == "*") this.pages
    else this.pages.filter(_.name == keyStr)

    if (pages.size > 1) throw new UnsupportedOperationException("Ambiguous key referring to multiple pages")
    else if (pages.size == 0) None
    else Some(pages(0))
  }

  def getUnstructured(keyStr: String): Option[Unstructured] = {

    val page = getPage(keyStr)
    val value = get(keyStr).flatMap {
      case u: Unstructured => Option(u)
      case _ => None
    }

    if (page.nonEmpty && value.nonEmpty) throw new UnsupportedOperationException("Ambiguous key referring to both page and data")
    else page.orElse(value)
  }

  def signature(exclude: Iterable[KeyLike]) = (this.cells -- exclude, pages.map(_.uid), pages.map(_.name))

  def asMap(): Map[String, Any] = this.cells
    .filterKeys(_.isInstanceOf[Key]).map(identity)
    .map( tuple => tuple._1.name -> tuple._2)

  def asJson(): String = Utils.toJson(this.asMap())

  //TODO: don't use any String that contains dot as column name, or you will encounter bug SPARK-2775
  //TODO: this will become the default extract at some point, but not now
//  //TODO: need special handling of Option[_]
//  def select(keys: Seq[KeyLike], fs: Seq[Expr[Any]]): PageRow = {
//
//    val newKVs = Map(
//      keys.zip(fs).flatMap{
//        tuple =>
//          val value = tuple._2(this)
//          value match {
//            case Some(v) => Some(tuple._1 -> v)
//            case None => None
//          }
//      }: _*
//    )
//    this.copy(cells = this.cells ++ newKVs)
//  }

  //TODO: don't use any String that contains dot as column name, or you will encounter bug SPARK-2775
  def select(fs: Seq[Expr[Any]]): PageRow = {
    val newKVs = fs.flatMap{
      f =>
        val value = f(this)
        value match {
          case Some(v) => Some(Key(f.name) -> v)
          case None => None
        }
    }
    this.copy(cells = this.cells ++ newKVs)
  }

  def selectTemp(fs: Seq[Expr[Any]]): PageRow = {
    val newKVs = fs.flatMap{
      f =>
        val value = f(this)
        value match {
          case Some(v) => Some(TempKey(f.name) -> v)
          case None => None
        }
    }
    this.copy(cells = this.cells ++ newKVs)
  }

  def remove(keys: Seq[KeyLike]): PageRow = {
    this.copy(cells = this.cells -- keys)
  }

  def filterKeys(f: KeyLike => Boolean): PageRow = {
    this.copy(cells = this.cells.filterKeys(f).map(identity))
  }

  def putPages(others: Seq[Page], joinType: JoinType): Option[PageRow] = {
    joinType match {
      case Inner =>
        if (others.isEmpty) None
        else Some(this.copy(pages = others))
      case LeftOuter =>
        Some(this.copy(pages = others))
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


  //retain old pageRow,
  //always left
  def flatten(
               keyStr: String,
               indexKey: Key,
               limit: Int,
               left: Boolean
               ): Seq[PageRow] = {

    val key = resolveKey(keyStr)

    import views._

    val newCells =cells.flattenKey(key, indexKey).slice(0, limit)

    if (left && newCells.isEmpty) {
      Seq(this.copy(cells = this.cells - key)) //this will make sure you dont't lose anything
    }
    else {
      newCells.map(newCell => this.copy(cells = newCell))
    }
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

  //only apply to last page
  //TODO: don't use any String that contains dot as column name, or you will encounter bug SPARK-2775
  //see https://issues.apache.org/jira/browse/SPARK-2775
  //  def extract(keyAndF: Seq[(String, Page => Any)]): PageRow = {
  //
  //    this.pages.lastOption match {
  //      case None => this
  //      case Some(page) =>
  //        val map = Map(
  //          keyAndF.map{
  //            tuple => (new Key(tuple._1), tuple._2(page))
  //          }: _*
  //        )
  //
  //        this.copy(cells = this.cells ++ map)
  //    }
  //  }

  //affect last page
  //TODO: deprecate? or enable pagekey
  def paginate(
                selector: String,
                attr: String,
                wget: Boolean,
                postActions: Seq[Action]
                )(
                limit: Int,
                indexKey: Key,
                flatten: Boolean
                )(
                spooky: SpookyContext
                ): Seq[PageRow] = {

    var currentRow = this
    var increment = this.pages.size

    while (currentRow.pages.size <= limit && increment > 0 && currentRow.pages.last.children(selector).attrs(attr, noEmpty = true).nonEmpty) {

      val action = if (!wget) Visit(new Value(currentRow.pages.last.children(selector).attrs(attr, noEmpty = true).head))
      else Wget(new Value(currentRow.pages.last.children(selector).attrs(attr, noEmpty = true).head))

      val newRow = currentRow.putPages(Trace(action::Nil).resolve(spooky), joinType = Merge).get

      increment = newRow.pages.size - currentRow.pages.size

      currentRow = newRow
    }

    if (flatten) currentRow.flattenPages("*",indexKey = indexKey)
    else Seq(currentRow)
  }
}