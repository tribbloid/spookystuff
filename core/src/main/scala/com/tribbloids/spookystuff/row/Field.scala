package com.tribbloids.spookystuff.row

import com.tribbloids.spookystuff.QueryException
import com.tribbloids.spookystuff.execution.AbstractExecutionPlan
import com.tribbloids.spookystuff.expressions._
import com.tribbloids.spookystuff.utils.IdentifierMixin

import scala.collection.mutable

abstract class ExpressionTransformer {
}

object Field {

  //  implicit def stringToReservedField(str: String) = Field(name = str, isReserved = true)
  //
  //  final val GROUPED_PAGE_FIELD = "S"
  //  final val GROUPED

  sealed abstract class ConflictResolving extends Serializable
  case object Error extends ConflictResolving
  case object Remove extends ConflictResolving
  case object Overwrite extends ConflictResolving

  def batchResolveConflict(
                            child: AbstractExecutionPlan,
                            exprs: Seq[Expression[Any]]
                          ): Seq[Expression[Any]] = {
    val resolvedExprs = exprs.map {
      expr =>
        resolveConflict(child, expr)
    }
    resolvedExprs
  }

  def resolveConflict(
                       child: AbstractExecutionPlan,
                       expr: Expression[Any]
                     ): Expression[Any] = {
    val resolvedField = expr.field.resolveConflict(child.fieldSet)
    expr ~ resolvedField
  }
}

/**
  * basic index in data row structure, similar to "Column" in Spark SQL but can refer to schemaless data as well.
  * DSL can implicitly convert symbol/SQL Column reference to this.
  */
case class Field(
                  name: String,
                  isWeak: Boolean = false,
                  // weak field can be referred by common expressions, but has lower priority
                  // weak field is removed when conflict resolving with an identical field
                  isInvisible: Boolean = false,
                  // invisible field cannot be referred by common expressions
                  // declare it to ensure that its value won't interfere with downstream execution.
                  isReserved: Boolean = false,

                  conflictResolving: Field.ConflictResolving = Field.Error,
                  isOrdinal: Boolean = false, //represents ordinal index in flatten/explore
                  depthRangeOption: Option[Range] = None, //represents depth in explore

                  metadata: mutable.Map[String, String] = mutable.Map()
                  //                  @transient modifierOpt: Option[Expression[Any] => Expression[Any]] = None, //cast to
                ) extends ExpressionTransformer with IdentifierMixin {

  lazy val _id = (name, isWeak, isInvisible, isReserved)

  def ! = this.copy(conflictResolving = Field.Overwrite)
  def * = this.copy(isWeak = true)
  def `#` = this.copy(isOrdinal = true)

  def isDepth = depthRangeOption.nonEmpty

  def isSortIndex: Boolean = isOrdinal || isDepth

  def suppressOutput = isWeak || isInvisible

  def resolveConflict(existing: Field): Field = {

    val effectiveCR = if (this.conflictResolving == Field.Overwrite) Field.Overwrite
    else if (existing.isWeak) Field.Remove
    else throw new QueryException(s"Field ${existing.name} already exist") //fail early

    this.copy(conflictResolving = effectiveCR)
  }

  def resolveConflict(existings: Set[Field]): Field = {

    val existingOpt = existings.find(_ == this)
    existingOpt.map {
      existing =>
        this.resolveConflict(existing)
    }
      .getOrElse(this)
  }

  override def toString = {
    val builder = StringBuilder.newBuilder
    builder append s"'$name"
    if (conflictResolving == Field.Overwrite) builder append " !"
    if (isWeak) builder append " *"
    if (isOrdinal) builder append " #"
    depthRangeOption.foreach(range => builder append s" [${range.head}...${range.last}]")
    builder.result()
  }

  //  override def transform[T](expr: Expression[T]): Expression[T] = {
  //
  //    val unwrapped = Alias.unwrap(expr)
  //
  //    new Alias[PageRow, Option[T]](unwrapped, this)
  //  }
}