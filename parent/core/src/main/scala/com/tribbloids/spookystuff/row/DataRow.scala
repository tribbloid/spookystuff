package com.tribbloids.spookystuff.row

import java.util.UUID
import com.tribbloids.spookystuff.utils.{Interpolation, SpookyUtils, SpookyViews}
import com.tribbloids.spookystuff.relay.{ProtoAPI, TreeIR}
import org.apache.spark.sql.catalyst.InternalRow

import scala.language.implicitConversions
import scala.reflect.ClassTag

object DataRow {

  type Data = InternalRow
  private val Data = InternalRow

  def fromKV(
      kvs: Iterable[(Field, Any)]
  ): DataRow#withFields = {

    val schema = SpookySchema(fields = kvs.map(_._1).toList)

    val data = Data.fromSeq(kvs.map(_._2).toSeq)

    DataRow(data).withFields(schema.allRefs: _*)
  }

  implicit def unbox(v: DataRow#withFields): DataRow = v.outer

  object WithSchema {

//    def apply(
//               row: DataRow,
//               schema: SpookySchema
//             ): WithSchema = {
//
//      WithSchema(row, schema)
//    }
  }
}

/**
  * data container that is persisted through different stages of execution plan. has no schema information, user are
  * required to refer to schema from driver and use the index number to access element. schema |x| field => index =>
  * value
  */
//TODO: change to wrap DataFrame Row/InternalRow?
//TODO: also carry PageUID & property type (Vertex/Edge) for GraphX or GraphFrame
@SerialVersionUID(6534469387269426194L)
case class DataRow(
    data: DataRow.Data = DataRow.Data.empty,
    groupID: Option[UUID] = None,
    groupIndex: Int = 0 // set to 0...n for each page group after SquashedPageRow.semiUnsquash/unsquash
) extends Serializable {

  import Field._

  @transient val sanity: Unit = {
    assert(data.isInstanceOf[Serializable]) // fail on construction but not on deserialization
  }

  //  def ++(m: Iterable[(Field.Index, Any)]): DataRow = this.copy(data = data ++ m)

  case class on(fieldReference: Ref) {

    lazy val cell: Option[Any] = Option(data.get(fieldReference.leftI))

    def typed[T <: Any: ClassTag]: Option[T] = cell.flatMap {
      SpookyUtils.typedOrNone[T]
    }

    lazy val int: Option[Int] = typed[Int]

    // T cannot <: AnyVal otherwise will run into https://issues.scala-lang.org/browse/SI-6967
    // getIntIterable cannot use it for the same reason
    def typedArray[T <: Any: ClassTag]: Option[Array[T]] = {
      val result = cell.map { v =>
        SpookyUtils.asArray[T](v)
      }
      result
    }

    lazy val array: Option[Array[Any]] = typedArray[Any]

    lazy val intArray: Option[Array[Int]] = typedArray[Int]

    lazy val intList: Option[List[Int]] = typedArray[Int].map(_.toList)

    def updated(v: Any): DataRow = {
      val newData = data.updated(fieldReference.leftI, v)

      DataRow.this.copy(data = newData)
    }

    // slower than updated
    def replaced(vs: Any*): DataRow = {

      val newData = data.map(v => Seq(v)).updated(fieldReference.leftI, vs.toSeq).flatten

      DataRow.this.copy(data = newData)
    }

    lazy val removed: DataRow = {
      replaced()
    }

    // sampler is applied AFTER ordinalIndex is generated
    // sometimes referred as "flatten", but it's inaccurate
    def explode(
        outer: Boolean,
        indexAlias: Option[Alias] = None,
        sampler: Sampler[Any] = identity
    ): Seq[DataRow] = {

      val v_indices = array.toSeq.flatMap(_.zipWithIndex)
      val sampled = sampler(v_indices)

      if (outer && sampled.isEmpty) {
        Seq(updated(null)) // you don't lose the remainder of a row because an element is empty
      } else {

        sampled.map { tuple =>
          updated(tuple)
        }
      }
    }
  }

  case class withFields(fieldReference: Ref*) {

    def outer: DataRow.this.type = DataRow.this

    lazy val removed: DataRow = {

      val iForRemoval = fieldReference.map(_.leftI).toSet
      val newData = data.zipWithIndex.filterNot { tuple =>
        iForRemoval.contains(tuple._2)
      }
      DataRow.this.copy(data = newData)
    }

    lazy val sortIndex: Seq[Option[List[Int]]] = {
      val result = fieldReference.map(r => on(r).intList)
      result
    }
//    lazy val toMap: Map[String, Any] = {
//
//      val seq = ref.map {
//        rr =>
//          rr.field
//      }
//
//      data.view
//        .filterKeys(_.isSelected)
//        .map(identity)
//        .map(tuple => tuple._1.name -> tuple._2)
//        .toMap
//    }
//
//    override def toMessage_>> : TreeIR.Leaf[Map[String, Any]] = {
//
//      TreeIR.leaf(toMap)
//    }
  }

  def withSchema(schema: SpookySchema): withFields = {

    withFields(schema.allRefs: _*)
  }

  def asSquashedRow: SquashedRow = SquashedRow(Array(this))
  // replace each '{key} in a string with their respective value in cells
//  def replaceInto(
//      str: String,
//      interpolation: Interpolation = Interpolation.`'`
//  ): Option[String] = {
//
//    try {
//      Option(
//        interpolation
//          .Compile(str) { key =>
//            val field = Alias(key)
//            "" + this.get(field).opt
//          }
//          .run()
//      )
//    } catch {
//      case _: NoSuchElementException => None
//    }
//  }
}
