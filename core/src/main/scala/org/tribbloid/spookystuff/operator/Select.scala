package org.tribbloid.spookystuff.operator

import org.tribbloid.spookystuff.entity.{Page, PageRow}

/**
 * Created by peng on 10/11/14.
 * This is the preferred way of defining extraction
 */
abstract class Select[T] extends (PageRow => T)  with Serializable {

  var name: String = this.toString()

  def as(name: Symbol): this.type = {

    this.name = name.name
    this
  }
}

//abstract class Extract[T] extends Select[T] {
//
//  final override def apply(pageRow: PageRow) = {
//    pageApply(filter(pageRow.pages))
//  }
//
//  var filter: Seq[Page] => Page
//
//  def pageApply(page: Page): T
//
//  def from(vv: Symbol): this.type = {
//    val vvStr = vv.name
//
//    if (vvStr == "*") {
//      filter = _.last
//    }
//    else {
//      filter = _.filter(page => page.alias == vvStr).last
//    }
//    this
//  }
//
//  val selectors: Seq[String] = Seq() //used to delay for element to exist
//}

case class FromPage[T](from: String = "*", extract: Extract[T]) extends Select[T] {

  def from(from: Symbol): FromPage[T] = this.copy(from = from.name)

  private def filter(pages: Seq[Page]): Page = {

    if (from == "*") {
      pages.last
    }
    else {
      pages.filter(page => page.name == from).last
    }
  }

  override def apply(pageRow: PageRow) = extract(filter(pageRow.pages))
}

case class FromCell(key: String) extends Select[Any] {

  override def apply(pageRow: PageRow) = pageRow(key)
}