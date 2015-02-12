package org.tribbloid.spookystuff.entity

/**
 * Created by peng on 11/7/14.
 */

trait KeyLike extends Serializable {
  val name: String
}

trait SortKey {
  this: KeyLike =>
}

trait HiddenKey {
  this: KeyLike =>
}

case class Key(override val name: String) extends KeyLike

object Key{

  def apply(sym: Symbol): Key = Option(sym).map(v => new Key(v.name)).orNull

  def sortKey(str: String): Key = Option(str).map(v => new Key(str) with SortKey).orNull

  def sortKey(sym: Symbol): Key = Option(sym).map(v => new Key(v.name) with SortKey).orNull
}

case class TempKey(override val name: String) extends KeyLike

object TempKey{

  def apply(sym: Symbol): TempKey = Option(sym).map(v => new TempKey(v.name)).orNull
}