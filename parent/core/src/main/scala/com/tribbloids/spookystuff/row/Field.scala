package com.tribbloids.spookystuff.row

import ai.acyclic.prover.commons.same.EqualBy
import com.tribbloids.spookystuff.QueryException
import com.tribbloids.spookystuff.relay.{ProtoAPI, TreeIR}
import com.tribbloids.spookystuff.row.Field.ConflictResolving

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
    isTransient: Boolean = false,
    // can be referred by extractions, but has lower priority
    // is removed when conflict resolving with an identical field
    //    isTemporary: Boolean = false, // TODO: enable
    // temporary fields should be evicted after every ExecutionPlan
    isReserved: Boolean = false,
    conflictResolving: Field.ConflictResolving = Field.Error,
    // TODO, this entire conflict resolving mechanism should be moved to typed Extractor, along with "isTransient"
    isOrdinal: Boolean = false, // represents ordinal index in flatten/explore
    depthRangeOpt: Option[Range] = None
    // represents depth in explore, at this moment, it doesn't affect engine execution
) extends EqualBy
    with ProtoAPI {

  lazy val samenessDelegatedTo: List[Any] = List(
    name,
    isTransient,
//    isTemporary,
    isReserved
  )

  def ! : Field = this.copy(conflictResolving = Field.ReplaceIfNotNull)
  def !! : Field = this.copy(conflictResolving = Field.Replace)
  def * : Field = this.copy(isTransient = true)
  def `#`: Field = this.copy(isOrdinal = true)

  def isDepth: Boolean = depthRangeOpt.nonEmpty
  def isSortIndex: Boolean = isOrdinal || isDepth

  def isNonTransient: Boolean = !isTransient

  def effectiveConflictResolving(existing: Field): ConflictResolving = {

    assert(this == existing)

    val effectiveCR = this.conflictResolving match {
      case Field.ReplaceIfNotNull =>
        Field.ReplaceIfNotNull
      case Field.Replace =>
        Field.Replace
      case _ => // Field.Error
        if (existing.isTransient) Field.Replace
        else throw new QueryException(s"Field '${existing.name}' already exist") // fail early
    }

    effectiveCR
  }

  override def toString: String = toMessage_>>.body

  override def toMessage_>> : TreeIR.Leaf[String] = {
    val builder = StringBuilder.newBuilder
    builder append s"'$name"
    if (conflictResolving == Field.ReplaceIfNotNull) builder append " !"
    if (isTransient) builder append " *"
    if (isOrdinal || isDepth) builder append " #"
    TreeIR.Builder(Some(name)).leaf(builder.result())
  }
}
