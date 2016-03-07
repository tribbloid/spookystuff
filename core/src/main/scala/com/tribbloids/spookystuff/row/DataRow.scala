package com.tribbloids.spookystuff.row

import java.util.UUID

import com.tribbloids.spookystuff.Const
import com.tribbloids.spookystuff.utils.{Utils, Views}

import scala.collection.Map
import scala.reflect.ClassTag

/**
  * makeshift data container that is persisted through different stages of execution plan
  */
//TODO: change to wrap DataFrame Row?
//TODO: also carry PageUID & property type (Vertex/Edge) for GraphX
case class DataRow(
                    values: Map[Field, Any] = Map(),
                    groupID: Option[UUID] = None,
                    groupIndex: Int = 0, //set to 0...n for each page group after SquashedPageRow.semiUnsquash/unsquash
                    freeze: Boolean = false //if set to true PageRow.extract won't insert anything into it, used in merge/replace join
                  ) {

  import Views._

  def updated(k: Field, v: Any) = this.copy(values = values.updated(k, v))

  def ++(m: Iterable[(Field, Any)]) = this.copy(values = values ++ m)

  def --(m: Iterable[Field]) = this.copy(values = values -- m)

  def nameToField(name: String): Option[Field] = {
    Some(Field(name, isWeak = true)).filter(values.contains)
      .orElse {
        Some(Field(name, isWeak = false)).filter(values.contains)
      }
  }

  //TempKey precedes ordinary Key
  //TODO: in scala 2.10.x, T cannot <: AnyVal otherwise will run into https://issues.scala-lang.org/browse/SI-6967
  def getTyped[T <: Any : ClassTag](field: Field): Option[T] = {
    val res = values.get(field).flatMap {
      case v: T => Some(v)
      case _ => None
    }
    res
  }

  def getInt(field: Field): Option[Int] = getTyped[Int](field)

  def get(field: Field): Option[Any] = values.get(field)

  def toMap: Map[String, Any] = values
    .filterKeys(!_.suppressOutput)
    .map(identity)
    .map(tuple => tuple._1.name -> tuple._2)

  def toJSON: String = {
    import Views._

    Utils.toJson(this.toMap.canonizeKeysToColumnNames)
  }

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

    val newValues_Indices: Seq[(Map[Field, Any], Int)] = values.flattenByKey(field, sampler)

    if (left && newValues_Indices.isEmpty) {
      Seq(this.copy(values = values - field)) //you don't lose the remainder of a row because an element is empty
    }
    else {
      val result: Seq[(DataRow, Int)] = newValues_Indices.map(tuple => this.copy(values = tuple._1) -> tuple._2)

      Option(ordinalField) match {
        case None => result.map(_._1)
        case Some(_field) =>
          val newValues: Seq[DataRow] = result.map {
            tuple =>
              val existingOpt: Option[Iterable[Any]] = tuple._1.get(_field).map{v => Utils.asIterable[Any](v)}
              val values = existingOpt match {
                case None =>
                  tuple._1.values.updated(_field, Array(tuple._2))
                case Some(existing) =>
                  tuple._1.values.updated(_field, (existing ++ Iterable(tuple._2)).toArray)
              }
              tuple._1.copy(values = values)
          }
          newValues
      }
    }
  }

  def clearWeakValues: DataRow = {
    val tempFields = this.values.keys.filter(_.isWeak == true)
    this.values -- tempFields
    this
  }

  //T cannot <: AnyVal otherwise will run into https://issues.scala-lang.org/browse/SI-6967
  //getIntIterable cannot use it for the same reason
  def getTypedArray[T <: Any: ClassTag](field: Field): Option[Array[T]] = {
    val res = values.get(field).map {
      v => Utils.asArray[T](v)
    }
    res
  }
  def getArray(field: Field): Option[Array[Any]] = getTypedArray[Any](field)
  def getIntArray(field: Field): Option[Array[Int]] = getTypedArray[Int](field)

  def getTypedIterable[T <: Any: ClassTag](field: Field): Option[Iterable[T]] = {
    val res = values.get(field).map {
      v => Utils.asIterable[T](v)
    }
    res
  }
  def getIterable(field: Field): Option[Iterable[Any]] = getTypedIterable[Any](field)
  def getIntIterable(field: Field): Option[Iterable[Int]] = getTypedIterable[Int](field)

  //replace each '{key} in a string with their respective value in cells
  def replaceInto(
                   str: String,
                   delimiter: String = Const.keyDelimiter
                 ): Option[String] = {

    if (str == null) return None
    if (str.isEmpty) return Some(str)

    val regex = (delimiter+"\\{[^\\{\\}\r\n]*\\}").r

    val result = regex.replaceAllIn(str,m => {
      val original = m.group(0)
      val key = original.substring(2, original.length-1)
      val field = Field(key)
      this.get(field) match {
        case Some(v) => v.toString
        case None => return None
      }
    })

    Some(result)
  }

  override def toString: String = values.toString()
}