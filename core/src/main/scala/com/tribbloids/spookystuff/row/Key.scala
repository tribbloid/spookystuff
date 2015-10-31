package com.tribbloids.spookystuff.row

/**
 * Created by peng on 11/7/14.
 */

trait KeyLike extends Serializable {
  val name: String
}

trait SortKeyMixin {
  this: KeyLike =>
}

trait OrdinalKeyMixin extends SortKeyMixin {
  this: KeyLike =>
}

trait DepthKeyMixin extends SortKeyMixin {
  this: KeyLike =>

  val maxOption: Option[Int]
}

trait HiddenKeyMixin extends SortKeyMixin {
  this: KeyLike =>
}

case class Key(override val name: String) extends KeyLike

object Key{

  def apply(sym: Symbol): Key = Option(sym).map(v => new Key(v.name)).orNull

  def sortKey(sym: Symbol): Key = Option(sym).map(v => new Key(v.name) with SortKeyMixin).orNull

  def ordinalKey(sym: Symbol): Key = Option(sym).map(v => new Key(v.name) with OrdinalKeyMixin).orNull

  def depthKey(sym: Symbol): Key = Option(sym).map(
    v =>
      new Key(v.name) with DepthKeyMixin {
        override val maxOption: Option[Int] = None
      }
  ).orNull

  def depthKey(sym: Symbol, max: Int): Key = Option(sym).map(
    v =>
      new Key(v.name) with DepthKeyMixin {
        override val maxOption: Option[Int] = Some(max)
      }
  ).orNull

  def hiddenKey(sym: Symbol): Key = Option(sym).map(v => new Key(v.name) with HiddenKeyMixin).orNull
}

case class TempKey(override val name: String) extends KeyLike

object TempKey{

  def apply(sym: Symbol): TempKey = Option(sym).map(v => new TempKey(v.name)).orNull
}