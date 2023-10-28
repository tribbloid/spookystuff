package com.tribbloids.spookystuff.row

import com.tribbloids.spookystuff.relay.{ProtoAPI, TreeIR}
import com.tribbloids.spookystuff.utils.EqualBy

import scala.language.implicitConversions

object Alias {

  implicit def fromString(str: String): Alias = Alias(str)

  implicit def fromSymbol(sym: Symbol): Alias = Alias(sym.name)

  implicit def fromField(field: Field): Alias = field.alias
}

/**
  * basic index in data row structure, similar to "Column" in Spark SQL but can refer to schemaless data as well. DSL
  * can implicitly convert symbol/SQL Column reference to this.
  */
case class Alias(
    name: String,
    isWeak: Boolean = false, // TODO: should be `isTemporary`
    // weak field can be referred by extractions, but has lower priority
    // weak field is removed when conflict resolving with an identical field
//    isTemporary: Boolean = false, // TODO: enable
    // temporary fields should be evicted after every ExecutionPlan
    isReserved: Boolean = false,
    // TODO, this entire conflict resolving mechanism should be moved to typed Extractor, along with isWeak
    isOrdinal: Boolean = false, // represents ordinal index in flatten/explore
    depthRangeOpt: Option[Range] = None, // represents depth in explore

    isSelectedOverride: Option[Boolean] = None
) extends Field.Symbol
    with EqualBy
    with ProtoAPI {

  lazy val _equalBy: List[Any] = List(
    name,
    isWeak,
//    isTemporary,
    isReserved
  )

  def * = this.copy(isWeak = true)
  def `#` = this.copy(isOrdinal = true)

  def isDepth: Boolean = depthRangeOpt.nonEmpty
  def isSortIndex: Boolean = isOrdinal || isDepth

  def isSelected: Boolean = isSelectedOverride.getOrElse(!isWeak)

  override def toString: String = toMessage_>>.body

  override def toMessage_>> : TreeIR.Leaf[String] = {
    val builder = StringBuilder.newBuilder
    builder append s"'$name"
    if (isWeak) builder append " *"
    if (isOrdinal) builder append " #"
    depthRangeOpt.foreach(range => builder append s" [${range.head}...${range.last}]")
    TreeIR.Builder(Some(name)).leaf(builder.result())
  }

  override def alias: Alias = this
}
