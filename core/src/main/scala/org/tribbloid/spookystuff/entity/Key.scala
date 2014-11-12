package org.tribbloid.spookystuff.entity

/**
 * Created by peng on 11/7/14.
 */

trait KeyLike{
  val name: String
}

case class Key(override val name: String) extends KeyLike

object Key{

  def apply(sym: Symbol): Key = Option(sym).map(_.name).map(new Key(_)).orNull
}

case class TempKey(override val name: String) extends KeyLike

object TempKey{

  def apply(sym: Symbol): TempKey = Option(sym).map(_.name).map(new TempKey(_)).orNull
}

case object NoKey extends KeyLike {
  override val name: String = null
}