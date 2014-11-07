package org.tribbloid.spookystuff.expressions

import org.tribbloid.spookystuff.entity.PageRow

/**
 * Created by peng on 10/11/14.
 * This is the preferred way of defining extraction
 * entry point for all "query lambda"
 */

trait Expr[T] extends (PageRow => T) {

  //this won't be rendered unless used against a PageRow
  def value: T = throw new UnsupportedOperationException("PENDING")

  def name: String = "Expr-" + this.hashCode()

  override def toString(): String = name
}

//just a simple wrapper for T, this is the only way to execute a action
//this is the only Expression that can be shipped remotely
final case class Value[T](override val value: T) extends Expr[T] {//all select used in query cannot have name changed

  override def apply(v1: PageRow): T = value

  override def name: String = value.toString
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

//case class LastPage[T](
//                                extract: Extract[T],
//                                from: String = "*",
//                                noPage: T = null
//                                ) extends Expr[T] {
//
//  def from(from: String): LastPage[T] = new LastPage(extract, from)
//
//  private def filter(pages: Seq[Page]): Option[Page] = {
//
//    if (from == "*") {
//      pages.lastOption
//    }
//    else {
//      pages.filter(page => page.name == from).lastOption //TODO: by default, will add more options later
//    }
//  }
//
//  override def apply(pageRow: PageRow) = filter(pageRow.pages) match {
//    case Some(page) => extract(page)
//    case _ => noPage
//  }
//
//  override def toString() = from + "[last]." + extract.toString()
//}

//case class FromPagesExpr[T](extract: (Page => Seq[T]), from: String = "*") extends Expression[T] {
//
//  def from(from: String): FromLastPageExpr[T] = new FromLastPageExpr(extract, from)
//
//  private def filter(pages: Seq[Page]): Seq[Page] = {
//
//    if (from == "*") {
//      pages
//    }
//    else {
//      pages.filter(page => page.name == from) //TODO: by default, will add more options later
//    }
//  }
//
//  override def apply(pageRow: PageRow) = filter(pageRow.pages).flatMap(extract)
//
//  override def toString() = from + "." + extract.toString()
//}

//class DelegateSelect[T](val self: PageRow => T) extends Select[T] {
//
//  override def apply(v1: PageRow): T = self(v1)
//
//  override def toString() = self.toString()
//}

//class FromCellSelect(key: String) extends Select[Any] {
//
//  override def apply(pageRow: PageRow) = pageRow(key)
//
//  override def toString() = "#{"+key+"}"
//}