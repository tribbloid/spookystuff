package com.tribbloids.spookystuff.row

import java.util.UUID

import com.tribbloids.spookystuff.utils.{Interpolation, SpookyUtils, SpookyViews}
import org.apache.spark.ml.dsl.utils.messaging.ProtoAPI

import scala.reflect.ClassTag

/**
  * data container that is persisted through different stages of execution plan.
  * has no schema information, user are required to refer to schema from driver and use the index number to access element.
  * schema |x| field => index => value
  */
//TODO: change to wrap DataFrame Row/InternalRow?
//TODO: also carry PageUID & property type (Vertex/Edge) for GraphX or GraphFrame
@SerialVersionUID(6534469387269426194L)
case class DataRow(
    data: Data = Data.empty,
    groupID: Option[UUID] = None,
    groupIndex: Int = 0, //set to 0...n for each page group after SquashedPageRow.semiUnsquash/unsquash
    freeze: Boolean = false //if set to true PageRow.extract won't insert anything into it, used in merge/replace join
) extends AbstractSpookyRow
    with ProtoAPI {

  {
    assert(data.isInstanceOf[Serializable]) //fail early
  }

  import SpookyViews._

  //  def copy(//TODO: remove, already in case class contract
  //            data: Data = this.data,
  //            groupID: Option[UUID] = this.groupID,
  //            groupIndex: Int = this.groupIndex, //set to 0...n for each page group after SquashedPageRow.semiUnsquash/unsquash
  //            freeze: Boolean = this.freeze //if set to true PageRow.extract won't insert anything into it, used in merge/replace join
  //          ) = DataRow(data, groupID, groupIndex, freeze)

  def ++(m: Iterable[(Field, Any)]): DataRow = this.copy(data = data ++ m)

  def --(m: Iterable[Field]): DataRow = this.copy(data = data -- m)

  def nameToField(name: String): Option[Field] = {
    Some(Field(name, isWeak = true))
      .filter(data.contains)
      .orElse {
        Some(Field(name)).filter(data.contains)
      }
  }

  def getTyped[T <: Any: ClassTag](field: Field): Option[T] = {
    data.get(field).flatMap {
      SpookyUtils.typedOrNone[T]
    }
  }
  def orWeakTyped[T <: Any: ClassTag](field: Field): Option[T] = {
    getTyped[T](field)
      .orElse(getTyped[T](field.*))
  }

  def get(field: Field): Option[Any] = data.get(field)
  def orWeak(field: Field): Option[Any] = orWeakTyped[Any](field)

  def getInt(field: Field): Option[Int] = getTyped[Int](field)

  lazy val toMap: Map[String, Any] = data
    .filterKeys(_.isSelected)
    .map(identity)
    .map(tuple => tuple._1.name -> tuple._2)

  override def toMessage_>> : Map[String, Any] = toMap

  def sortIndex(fields: Seq[Field]): Seq[Option[Iterable[Int]]] = {
    val result = fields.map(key => this.getIntIterable(key))
    result
  }

  //retain old pageRow,
  //always left
  //TODO: add test to ensure that ordinalField is added BEFORE sampling
  def flatten(
      field: Field,
      ordinalField: Field,
      left: Boolean,
      sampler: Sampler[Any]
  ): Seq[DataRow] = {

    val newValues_Indices = data.flattenByKey(field, sampler)

    if (left && newValues_Indices.isEmpty) {
      Seq(this.copy(data = data - field)) //you don't lose the remainder of a row because an element is empty
    } else {
      val result: Seq[(DataRow, Int)] = newValues_Indices.map(tuple => this.copy(data = tuple._1.toMap) -> tuple._2)

      Option(ordinalField) match {
        case None => result.map(_._1)
        case Some(_field) =>
          val newValues: Seq[DataRow] = result.map { tuple =>
            val existingOpt: Option[Iterable[Any]] = tuple._1.get(_field).map { v =>
              SpookyUtils.asIterable[Any](v)
            }
            val values = existingOpt match {
              case None =>
                tuple._1.data.updated(_field, Array(tuple._2))
              case Some(existing) =>
                tuple._1.data.updated(_field, (existing ++ Iterable(tuple._2)).toArray)
            }
            tuple._1.copy(data = values)
          }
          newValues
      }
    }
  }

  //  def withoutWeak: DataRow = {
  //    val tempFields = this.data.keys.filter(_.isWeak == true)
  //    this -- tempFields
  //  }

  //T cannot <: AnyVal otherwise will run into https://issues.scala-lang.org/browse/SI-6967
  //getIntIterable cannot use it for the same reason
  def getTypedArray[T <: Any: ClassTag](field: Field): Option[Array[T]] = {
    val result = data.get(field).map { v =>
      SpookyUtils.asArray[T](v)
    }
    result
  }
  def getArray(field: Field): Option[Array[Any]] = getTypedArray[Any](field)

  //TODO: cleanup getters after this line, they are useless
  def getIntArray(field: Field): Option[Array[Int]] = getTypedArray[Int](field)

  def getTypedIterable[T <: Any: ClassTag](field: Field): Option[Iterable[T]] = {
    val res = data.get(field).map { v =>
      SpookyUtils.asIterable[T](v)
    }
    res
  }
  def getIterable(field: Field): Option[Iterable[Any]] = getTypedIterable[Any](field)
  def getIntIterable(field: Field): Option[Iterable[Int]] = getTypedIterable[Int](field)

  //replace each '{key} in a string with their respective value in cells
  def replaceInto(
      str: String,
      interpolation: Interpolation = Interpolation.`'`
  ): Option[String] = {

    try {
      Option(
        interpolation
          .Compile(str) { key =>
            val field = Field(key)
            "" + this.get(field).get
          }
          .run()
      )
    } catch {
      case e: NoSuchElementException => None
    }
  }

  //  override def toString: String = data.toString()
}
