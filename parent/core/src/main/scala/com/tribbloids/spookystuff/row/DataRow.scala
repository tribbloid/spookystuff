package com.tribbloids.spookystuff.row

import com.tribbloids.spookystuff.dsl.ForkType

import java.util.UUID
import com.tribbloids.spookystuff.utils.{SpookyUtils, SpookyViews}
import com.tribbloids.spookystuff.relay.{ProtoAPI, TreeIR}

import scala.reflect.ClassTag

/**
  * contains all schematic data accumulated over graph traversal path, but contains no schema, ad-hoc local access
  * requires combining with schema from Spark driver
  *
  * @param data
  *   internal representation
  * @param exploreLineageID
  *   only used in [[com.tribbloids.spookystuff.dsl.ExploreAlgorithm]], multiple [[DataRow]] with identical
  *   [[exploreLineageID]] are assumed to be scrapped from the same graph traversal path, in most reduction algorithms
  *   these rows are treated as an inseparable unit
  */
@SerialVersionUID(6534469387269426194L)
case class DataRow(
    data: Data = Data.empty,
    exploreLineageID: Option[UUID] = None
    // TODO: I don't like this explanation, give me something better
) extends ProtoAPI {
  // TODO: will become TypedRow that leverage extensible Record and frameless
  // TODO: how to easily reconstruct vertices/edges for graphX/graphframe?

  {
    assert(data.isInstanceOf[Serializable]) // fail early
  }

  import SpookyViews._

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

  lazy val toMap: Map[String, Any] = data.view
    .filterKeys(_.isNonTransient)
    .map(identity)
    .map(tuple => tuple._1.name -> tuple._2)
    .toMap

  override def toMessage_>> : TreeIR.Leaf[Map[String, Any]] = {

    TreeIR.leaf(toMap)
  }

  def sortIndex(fields: Seq[Field]): Seq[Option[Iterable[Int]]] = {
    val result = fields.map(key => this.getIntIterable(key))
    result
  }

  // retain old pageRow,
  // always left
  // TODO: add test to ensure that ordinalField is added BEFORE sampling
  def explode(
      field: Field,
      ordinalField: Field,
      forkType: ForkType,
      sampler: Sampler[Any]
  ): Seq[DataRow] = {

    val newValues_Indices = data.explode1Key(field, sampler)

    if (forkType.isLeft && newValues_Indices.isEmpty) {
      Seq(this.copy(data = data - field)) // you don't lose the remainder of a row because an element is empty
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

  // T cannot <: AnyVal otherwise will run into https://issues.scala-lang.org/browse/SI-6967
  // getIntIterable cannot use it for the same reason
  def getTypedArray[T <: Any: ClassTag](field: Field): Option[Array[T]] = {
    val result = data.get(field).map { v =>
      SpookyUtils.asArray[T](v)
    }
    result
  }
  def getArray(field: Field): Option[Array[Any]] = getTypedArray[Any](field)

  // TODO: cleanup getters after this line, they are useless
  def getIntArray(field: Field): Option[Array[Int]] = getTypedArray[Int](field)

  def getTypedIterable[T <: Any: ClassTag](field: Field): Option[Iterable[T]] = {
    val res = data.get(field).map { v =>
      SpookyUtils.asIterable[T](v)
    }
    res
  }
  def getIterable(field: Field): Option[Iterable[Any]] = getTypedIterable[Any](field)
  def getIntIterable(field: Field): Option[Iterable[Int]] = getTypedIterable[Int](field)
}
