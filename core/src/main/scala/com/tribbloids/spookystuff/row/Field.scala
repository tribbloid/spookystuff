package com.tribbloids.spookystuff.row

import com.tribbloids.spookystuff.QueryException
import com.tribbloids.spookystuff.utils.IdentifierMixin

import scala.language.implicitConversions

//abstract class ExpressionTransformer {
//}

object Field {

  //  implicit def stringToReservedField(str: String) = Field(name = str, isReserved = true)
  //
  //  final val GROUPED_PAGE_FIELD = "S"
  //  final val GROUPED

  sealed abstract class ConflictResolving extends Serializable
  case object Error extends ConflictResolving
  case object Remove extends ConflictResolving
  case object Overwrite extends ConflictResolving

  implicit def str2Field(str: String): Field = Field(str)
}

/**
  * basic index in data row structure, similar to "Column" in Spark SQL but can refer to schemaless data as well.
  * DSL can implicitly convert symbol/SQL Column reference to this.
  */
case class Field(
                  name: String,

                  isWeak: Boolean = false,
                  // weak field can be referred by common extractions, but has lower priority
                  // weak field is removed when conflict resolving with an identical field
                  isInvisible: Boolean = false,
                  // invisible field cannot be referred by common extractions
                  // declare it to ensure that its value won't interfere with downstream execution.
                  isReserved: Boolean = false,

                  conflictResolving: Field.ConflictResolving = Field.Error,
                  isOrdinal: Boolean = false, //represents ordinal index in flatten/explore
                  depthRangeOpt: Option[Range] = None //represents depth in explore

                //TODO: make no longer optional?
                ) extends IdentifierMixin {

  lazy val _id = (name, isWeak, isInvisible, isReserved)

  def ! = this.copy(conflictResolving = Field.Overwrite)
  def * = this.copy(isWeak = true)
  def `#` = this.copy(isOrdinal = true)

  def isDepth = depthRangeOpt.nonEmpty

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
    depthRangeOpt.foreach(range => builder append s" [${range.head}...${range.last}]")
    builder.result()
  }

//  def addType[RT: TypeTag](): Field = this.copy(
//    typeTagOpt = Some(typeTag[RT])
//  )
//
//  lazy val dataTypeOpt = {
//    this.typeTagOpt.map{
//      typeTag =>
//        ScalaReflection.schemaFor(typeTag).dataType
//    }
//  }
//
//  @transient lazy val structField = {
//    val dataType = dataTypeOpt.get // throw an exception when not typed
//    StructField(
//      name,
//      dataType,
//      nullable = true,
//      metadata
//    )
//  }
}