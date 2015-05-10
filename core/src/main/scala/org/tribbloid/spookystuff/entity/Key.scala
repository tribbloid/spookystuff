package org.tribbloid.spookystuff.entity

/**
 * Created by peng on 11/7/14.
 */

trait KeyLike extends Serializable {
  val name: String
}

trait SortKeyHelper {
  this: KeyLike =>
}

trait HiddenKeyHelper extends SortKeyHelper {
  this: KeyLike =>
}

case class Key(override val name: String) extends KeyLike

object Key{

  def apply(sym: Symbol): Key = Option(sym).map(v => new Key(v.name)).orNull

  def sortKey(str: String): Key = Option(str).map(v => new Key(str) with SortKeyHelper).orNull

  def sortKey(sym: Symbol): Key = Option(sym).map(v => new Key(v.name) with SortKeyHelper).orNull

  def hiddenKey(str: String): Key = Option(str).map(v => new Key(str) with HiddenKeyHelper).orNull

  def hiddenKey(sym: Symbol): Key = Option(sym).map(v => new Key(v.name) with HiddenKeyHelper).orNull
}

case class TempKey(override val name: String) extends KeyLike

object TempKey{

  def apply(sym: Symbol): TempKey = Option(sym).map(v => new TempKey(v.name)).orNull
}