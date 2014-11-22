package org.tribbloid.spookystuff.expressions

import org.tribbloid.spookystuff.entity.PageRow

/**
 * Created by peng on 10/11/14.
 * This is the preferred way of defining extraction
 * entry point for all "query lambda"
 */

//name: target column
//
trait Expr[+T] extends (PageRow => T) with Serializable {

  //this won't be rendered unless used against a PageRow
  def value: T = throw new UnsupportedOperationException("PENDING")

  final var name: String = this.hashCode().toString

  def as(name: Symbol): this.type = {
    assert(name != null)

    this.name = name.name
    this
  }

  final override def toString(): String = name
}

object Expr {

  def apply[T](f: PageRow => T) = f.asInstanceOf[PageRow => T with Expr[T]]

  def apply[T](value: T) = Value[T](value)
}

//just a simple wrapper for T, this is the only way to execute a action
//this is the only Expression that can be shipped remotely
final case class Value[T](override val value: T) extends Expr[T] {//all select used in query cannot have name changed

  name = value.toString

//  override def as(name: Symbol): this.type = throw new UnsupportedOperationException("cannot change name of value")

  override def apply(v1: PageRow): T = value
}

final case class ByKeyExpr(keyName: String) extends Expr[Any] {

  name = keyName

  override def apply(v1: PageRow): Any =
    v1.get(keyName)

  def href(selector: String,
           absolute: Boolean = true,
           noEmpty: Boolean = true
            ): FromPageExpr[Seq[String]] = new FromPageExpr(keyName, Href(selector, absolute, noEmpty))

  def src(selector: String,
          absolute: Boolean = true,
          noEmpty: Boolean = true
           ): FromPageExpr[Seq[String]] = new FromPageExpr(keyName, Src(selector, absolute, noEmpty))
}

final case class FromPageExpr[T](pageKey: String, extract: Extract[T]) extends Expr[T] {

  name = pageKey +"." + extract.toString()

  override def apply(v1: PageRow): T = {
    val pages = v1.getPages(pageKey)

    if (pages.size > 1) throw new UnsupportedOperationException("multiple pages with the same name, flatten first")
    else if (pages.size == 0) return null.asInstanceOf[T]

    extract(pages(0))
  }
}