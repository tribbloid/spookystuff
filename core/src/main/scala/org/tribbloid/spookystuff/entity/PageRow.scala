package org.tribbloid.spookystuff.entity

import org.tribbloid.spookystuff.SpookyContext
import org.tribbloid.spookystuff.actions._
import org.tribbloid.spookystuff.expressions._
import org.tribbloid.spookystuff.factory.PageBuilder
import org.tribbloid.spookystuff.utils.{Const, Utils}

import scala.collection.mutable.ArrayBuffer

/**
 * Created by peng on 8/29/14.
 */
//TODO: verify this! document is really scarce
//The precedence of an inﬁx operator is determined by the operator’s ﬁrst character.
//Characters are listed below in increasing order of precedence, with characters on
//the same line having the same precedence.
//(all letters)
//|
//^
//&
//= !.................................................(new doc)
//< >
//= !.................................................(old doc)
//:
//+ -
//* / %
//(all other special characters)
//now using immutable pattern to increase maintainability
//put all narrow transformation closures here
case class PageRow(
                    cells: Map[String, Any] = Map(),
                    pages: Seq[Page] = Seq(),
                    actions: Seq[Action] = Seq(),
                    dead: Boolean = false
                    )
  extends Serializable {

  def apply(key: String): Any = this.cells.getOrElse(key, null)

  def +>(a: Action): PageRow = {
    if (!this.dead) {
      this.copy(actions = this.actions :+ a.interpolate(this).get)
    }
    else {
      this
    }
  }

  def +>(as: Seq[Action]): PageRow = {
    if (!this.dead) {
      this.copy(actions = this.actions ++ as.map(_.interpolate(this).get))
    }
    else {
      this
    }
  }

  def +>(pr: PageRow): PageRow = {
    if (!this.dead) {
      this.copy(
        cells = this.cells ++ pr.cells,
        pages = this.pages ++ pr.pages,
        actions = this.actions ++ pr.actions.map(_.interpolate(this).get),
        dead = pr.dead
      )
    }
    else {
      this
    }
  }

  def die(): PageRow = {
    this.copy(dead = true)
  }

  def +*>(actions: Seq[_]): Array[PageRow] = {
    val results: ArrayBuffer[PageRow] = ArrayBuffer()

    for (action <- actions) {
      action match {
        case a: Action => results += (this +> a)
        case sa: Seq[_] => results += (this +> sa.filter(_.isInstanceOf[Action]).asInstanceOf[Seq[Action]])
        case pr: PageRow => results += (this +> pr)
        //        case am: (ClientAction, Map[String, Any]) => results += (this +> am._1).copy(cells = this.cells ++ am._2)
        //        case sam: (Seq[ClientAction], Map[String, Any]) => results += (this +> sam._1).copy(cells = this.cells ++ sam._2)
        case _ => throw new UnsupportedOperationException("Can only append Seq[ClientAction], Seq[Seq[ClientAction]] or Seq[PageRow]")
      }
    }

    results.toArray
  }

  def dropActions(): PageRow = {
    this.copy(actions = Seq(), dead = false)
  }

  //  def discardPage(): Unit = {
  //    this.page = None
  //  }

  //    def slice(
  //               selector: String,
  //               limit: Int = Const.fetchLimit,
  //               expand: Boolean = false,
  //               indexKey: String = null
  //               ): Array[PageRow] = {
  //
  //      val pages = this.page.get.slice(selector, limit, expand)
  //
  //      pages.zipWithIndex.map {
  //        tuple => {
  //
  //          PageRow(this.cells + (indexKey -> tuple._2) ,Some(tuple._1))
  //        }
  //      }
  //    }

  def asJson(): String = Utils.toJson(this.cells)

  def flatten(
               left: Boolean = false,
               indexKey: String = null
               ): Array[PageRow] = {
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

    if (left && result.isEmpty) {
      Array(this.copy(pages = Seq()))
    }
    else {
      result.toArray
    }
  }

  //only apply to last page
  //TODO: don't use any String that contains dot as column name, or you will encounter bug SPARK-2775
  //see https://issues.apache.org/jira/browse/SPARK-2775
  def extract(keyAndF: (String, Page => Any)*): PageRow = {

    this.pages.lastOption match {
      case None => this
      case Some(page) =>
        val map = Map(
          keyAndF.map{
            tuple => (tuple._1, tuple._2(page))
          }: _*
        )

        this.copy(cells = this.cells ++ map)
    }
  }

  //TODO: this will become the default extract at some point, but not now
  def select(keyAndF: (String, PageRow => Any)*): PageRow = {

    val map = Map(
      keyAndF.map {
        tuple => (tuple._1, tuple._2(this))
      }: _*
    )

    this.copy(cells = this.cells ++ map)
  }

  def remove(keys: String*): PageRow = {
    this.copy(cells = this.cells -- keys)
  }

  def +%>(
           actionAndF: (Action, Page => Any)
           ): PageRow = {

    this.pages.lastOption match {
      case None => this.die()
      case Some(page) => this +> this.pages.last.crawl1(actionAndF._1, actionAndF._2)
    }
  }

  //only apply to last page
  def +*%>(
            actionAndF: (Action, Page => Array[_])
            )(
            limit: Int, //applied after distinct
            distinct: Boolean = true,
            indexKey: String = null
            ): Array[PageRow] = {

    this.pages.lastOption match {
      case None => Array(this.die())
      case Some(page) => this +*> page.crawl(actionAndF._1, actionAndF._2)(limit, distinct, indexKey)
    }
  }

  def slice(
             selector: String,
             expand: Int = 0
             )(
             limit: Int, //applied after distinct
             indexKey: String = null,
             joinType: JoinType = Const.defaultJoinType,
             flatten: Boolean = true
             ): Array[PageRow] = {

    val sliced = this.pages.lastOption match {
      case None => Array[Page]()
      case Some(page) => page.slice(selector, expand)(limit)
    }

    val pages: Seq[Page] = joinType match {
      case Replace if sliced.isEmpty =>
        this.pages
      case Append =>
        this.pages ++ sliced
      case _ =>
        sliced
    }

    if (flatten) this.copy(pages = pages).flatten(joinType == LeftOuter, indexKey = indexKey)
    else Array(this.copy(pages = pages))
  }

  def !=!(
           joinType: JoinType = Const.defaultJoinType,
           flatten: Boolean = true,
           indexKey: String = null
           )(
           implicit spooky: SpookyContext
           ): Array[PageRow] = {

    val pages: Seq[Page] = joinType match {
      case Replace if this.actions.isEmpty =>
        this.pages
      case Append =>
        this.pages ++ PageBuilder.resolve(this.actions, this.dead)(spooky)
      case Merge =>
        val oldUids = this.pages.map(_.uid)
        val newPages = PageBuilder.resolve(this.actions, this.dead)(spooky).filter(newPage => !oldUids.contains(newPage.uid))
        this.pages ++ newPages
      case _ =>
        PageBuilder.resolve(this.actions, this.dead)(spooky)
    }

    if (flatten) PageRow(cells = this.cells, pages = pages).flatten(joinType == LeftOuter, indexKey)
    else Array(PageRow(cells = this.cells, pages = pages))
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
                indexKey: String,
                flatten: Boolean,
                last: Boolean
                )(
                implicit spooky: SpookyContext
                ): Array[PageRow] = {

    var currentRow = this.dropActions()
    var increment = currentRow.pages.size

    while (currentRow.pages.size <= limit && increment > 0 && currentRow.pages.last.attrExist(selector, attr)) {

      val actionRow = if (!wget) currentRow +%> (Visit("#{~}") -> (_.attr1(selector, attr, noEmpty = true, last = last)))
      else currentRow +%> (Wget("#{~}") -> (_.attr1(selector, attr, noEmpty = true, last = last)))

      val newRow = (actionRow +> postActions).!=!(joinType = Merge, flatten = false).head

      val oldSize = currentRow.pages.size

      currentRow = (actionRow +> postActions).!=!(joinType = Merge, flatten = false).head
      increment = currentRow.pages.size - oldSize
    }

    if (flatten) currentRow.flatten(left = true, indexKey = indexKey)
    else Array(currentRow)
  }
}

object DeadRow extends PageRow(dead = true)

object JoinVisit extends Visit("#{~}")

object JoinWget extends Wget("#{~}")