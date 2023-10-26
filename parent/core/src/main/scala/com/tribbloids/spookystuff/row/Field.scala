package com.tribbloids.spookystuff.row

import com.tribbloids.spookystuff.QueryException
import com.tribbloids.spookystuff.row.Field.ConflictResolving
import com.tribbloids.spookystuff.utils.EqualBy
import com.tribbloids.spookystuff.relay.{ProtoAPI, TreeIR}
import org.apache.spark.sql.types.{DataType, Metadata, StructField}

import scala.language.implicitConversions

object Field {

  /**
    * define whether to evict old values that has identical field name in previous table
    */
  sealed abstract class ConflictResolving extends Serializable

  // Fail fast
  case object Error extends ConflictResolving

  // Always evict old value
  case object Replace extends ConflictResolving

  // Only evict old value if the new value is not NULL.
  case object ReplaceIfNotNull extends ConflictResolving

  implicit def str2Field(str: String): Field = Field(str)

  implicit def symbol2Field(sym: Symbol): Field = Field(sym.name)
}

/**
  * basic index in data row structure, similar to "Column" in Spark SQL but can refer to schemaless data as well. DSL
  * can implicitly convert symbol/SQL Column reference to this.
  */
case class Field(
    name: String,
    isWeak: Boolean = false,
    // weak field can be referred by extractions, but has lower priority
    // weak field is removed when conflict resolving with an identical field
//    isTemporary: Boolean = false, // TODO: enable
    // temporary fields should be evicted after every ExecutionPlan
    isReserved: Boolean = false,
    conflictResolving: Field.ConflictResolving = Field.Error,
    // TODO, this entire conflict resolving mechanism should be moved to typed Extractor, along with isWeak
    isOrdinal: Boolean = false, // represents ordinal index in flatten/explore
    depthRangeOpt: Option[Range] = None, // represents depth in explore

    isSelectedOverride: Option[Boolean] = None
) extends EqualBy
    with ProtoAPI {

  lazy val _equalBy: List[Any] = List(
    name,
    isWeak,
//    isTemporary,
    isReserved
  )

  def ! = this.copy(conflictResolving = Field.ReplaceIfNotNull)
  def !! = this.copy(conflictResolving = Field.Replace)
  def * = this.copy(isWeak = true)
  def `#` = this.copy(isOrdinal = true)

  def isDepth: Boolean = depthRangeOpt.nonEmpty
  def isSortIndex: Boolean = isOrdinal || isDepth

  def isSelected: Boolean = isSelectedOverride.getOrElse(!isWeak)

  def effectiveConflictResolving(existing: Field): ConflictResolving = {

    assert(this == existing)

    val effectiveCR = this.conflictResolving match {
      case Field.ReplaceIfNotNull =>
        Field.ReplaceIfNotNull
      case Field.Replace =>
        Field.Replace
      case _ => // Field.Error
        if (existing.isWeak) Field.Replace
        else throw new QueryException(s"Field '${existing.name}' already exist") // fail early
    }

    effectiveCR
  }

  override def toString: String = toMessage_>>.body

  override def toMessage_>> : TreeIR.Leaf[String] = {
    val builder = StringBuilder.newBuilder
    builder append s"'$name"
    if (conflictResolving == Field.ReplaceIfNotNull) builder append " !"
    if (isWeak) builder append " *"
    if (isOrdinal) builder append " #"
    depthRangeOpt.foreach(range => builder append s" [${range.head}...${range.last}]")
    TreeIR.Builder(Some(name)).leaf(builder.result())
  }
}

//used to convert SquashedFetchedRow to DF
case class TypedField(
    self: Field,
    dataType: DataType,
    metaData: Metadata = Metadata.empty
) {

  def toStructField: StructField = StructField(
    self.name,
    dataType
  )
}
