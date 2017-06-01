package com.tribbloids.spookystuff.extractors

import com.tribbloids.spookystuff.doc._
import com.tribbloids.spookystuff.extractors.GenExtractor.{AndThen, Leaf, Static, StaticType}
import com.tribbloids.spookystuff.row.{DataRowSchema, _}
import com.tribbloids.spookystuff.utils.ScalaType._
import com.tribbloids.spookystuff.utils.{SpookyUtils, UnreifiedScalaType}
import org.apache.spark.ml.dsl.utils.MessageAPI
import org.apache.spark.sql.catalyst.ScalaReflection.universe.TypeTag
import org.apache.spark.sql.types._

import scala.collection.TraversableOnce
import scala.collection.immutable.ListMap
import scala.reflect.ClassTag

object Extractors {

  val GroupIndexExpr = GenExtractor.fromFn{
    (v1: FR) => v1.dataRow.groupIndex
  }

  //
  def GetUnstructuredExpr(field: Field): GenExtractor[FR, Unstructured] = GenExtractor.fromOptionFn {
    (v1: FR) =>
      v1.getUnstructured(field)
        .orElse{
          v1.getUnstructured(field.copy(isWeak = true))
        }
        .orElse {
          v1.getDoc(field.name).map(_.root)
        }
  }

  def GetDocExpr(field: Field) = GenExtractor.fromOptionFn {
    (v1: FR) => v1.getDoc(field.name)
  }
  val GetOnlyDocExpr = GenExtractor.fromOptionFn {
    (v1: FR) => v1.getOnlyDoc
  }
  val GetAllRootExpr = GenExtractor.fromFn {
    (v1: FR) => new Elements(v1.docs.map(_.root).toList)
  }

  case class FindAllMeta(arg: Extractor[Unstructured], selector: String)
  def FindAllExpr(arg: Extractor[Unstructured], selector: String) = arg.andFn(
    {
      v1: Unstructured => v1.findAll(selector)
    },
    Some(FindAllMeta(arg, selector))
  )

  case class ChildrenMeta(arg: Extractor[Unstructured], selector: String)
  def ChildrenExpr(arg: Extractor[Unstructured], selector: String) = arg.andFn(
    {
      v1 => v1.children(selector)
    },
    Some(ChildrenMeta(arg, selector))
  )

  def ExpandExpr(arg: Extractor[Unstructured], range: Range) = {
    arg match {
      case AndThen(_,_,Some(FindAllMeta(argg, selector))) =>
        argg.andFn(_.findAllWithSiblings(selector, range))
      case AndThen(_,_,Some(ChildrenMeta(argg, selector))) =>
        argg.andFn(_.childrenWithSiblings(selector, range))
      case _ =>
        throw new UnsupportedOperationException("expression does not support expand")
    }
  }

  def ReplaceKeyExpr(str: String) = GenExtractor.fromOptionFn {
    (v1: FR) =>
      v1.dataRow.replaceInto(str)
  }
}

object Lit {

  def apply[T: TypeTag](v: T): Lit[FR, T] = {
    apply[FR, T](v, UnreifiedScalaType.apply[T])
  }

  def erase[T](v: T): Lit[FR, T] = {
    apply[FR, T](v, NullType)
  }

  lazy val NULL: Lit[FR, Null] = erase(null)
}

//TODO: Message JSON conversion discard dataType info, is it wise?
case class Lit[T, +R](value: R, dataType: DataType)
  extends Static[T, R] with MessageAPI {

  def valueOpt: Option[R] = Option(value)
  override def toMessage = value

  override lazy val toString = valueOpt
    .map {
      v =>
        //        MessageView(v).toJSON(pretty = false)
        "" + v
    }
    .getOrElse("NULL")

  override val partialFunction: PartialFunction[T, R] = Unlift({ _: T => valueOpt})
}

case class GetExpr(field: Field) extends Leaf[FR, Any] {

  override def resolveType(tt: DataType): DataType = tt match {
    case schema: DataRowSchema =>
      schema
        .typedFor(field)
        .orElse{
          schema.typedFor(field.*)
        }
        .map(_.dataType)
        .getOrElse(NullType)
    case _ =>
      throw new UnsupportedOperationException("Can only resolve type against SchemaContext")
  }

  override def resolve(tt: DataType): PartialFunction[FR, Any] = Unlift(
    v =>
      v.dataRow.orWeak(field)
  )

  def GetSeqExpr: GenExtractor[FR, Seq[Any]] = this.andOptionTyped[Any, Seq[Any]](
    {
      case v: TraversableOnce[Any] => Some(v.toSeq)
      case v: Array[Any] => Some(v.toSeq)
      case _ => None
    },
    {
      _.ensureArray
    }
  )

  def AsSeqExpr: GenExtractor[FR, Seq[Any]] = this.andOptionTyped[Any, Seq[Any]](
    {
      case v: TraversableOnce[Any] => Some(v.toSeq)
      case v: Array[Any] => Some(v.toSeq)
      case v@ _ => Some(Seq(v))
    },
    {
      _.asArray
    }
  )
}

object AppendExpr {

  def create[T: ClassTag](
                           field: Field,
                           expr: Extractor[T]
                         ): Alias[FR, Seq[T]] = {

    AppendExpr[T](GetExpr(field), expr).withAlias(field.!!)
  }
}

case class AppendExpr[+T: ClassTag] private(
                                             get: GetExpr,
                                             expr: Extractor[T]
                                           ) extends Extractor[Seq[T]] {

  override def resolveType(tt: DataType): DataType = {
    val existingType = expr.resolveType(tt)

    existingType.asArray
  }

  override def resolve(tt: DataType): PartialFunction[FR, Seq[T]] = {
    val getSeqResolved = get.AsSeqExpr.resolve(tt).lift
    val exprResolved = expr.resolve(tt).lift

    PartialFunction({
      v1: FR =>
        val lastOption = exprResolved.apply(v1)
        val oldOption = getSeqResolved.apply(v1)

        oldOption.toSeq.flatMap{
          old =>
            SpookyUtils.asIterable[T](old)
        } ++ lastOption
    })
  }

  override def _args: Seq[GenExtractor[_, _]] = Seq(get, expr)
}

case class InterpolateExpr(parts: Seq[String], _args: Seq[Extractor[Any]]) extends Extractor[String] with StaticType[FR, String] {

  override def resolve(tt: DataType): PartialFunction[FR, String] = {
    val rs = _args.map(_.resolve(tt).lift)

    Unlift({
      row =>
        val iParts = parts.map(row.dataRow.replaceInto(_))

        val vs = rs.map(_.apply(row))
        val result = if (iParts.contains(None) || vs.contains(None)) None
        else Some(iParts.zip(vs).map(tpl => tpl._1.get + tpl._2.get).mkString + iParts.last.get)

        result
    })
  }

  override val dataType: DataType = StringType
}

//TODO: delegate to And_->
//TODO: need tests
case class ZippedExpr[T1,+T2](
                               arg1: Extractor[Iterable[T1]],
                               arg2: Extractor[Iterable[T2]]
                             )
  extends Extractor[Map[T1, T2]] {

  override val _args: Seq[GenExtractor[FR, _]] = Seq(arg1, arg2)

  override def resolveType(tt: DataType): DataType = {
    val t1 = arg1.resolveType(tt).unboxArrayOrMap
    val t2 = arg2.resolveType(tt).unboxArrayOrMap

    MapType(t1, t2)
  }

  override def resolve(tt: DataType): PartialFunction[FR, Map[T1, T2]] = {
    val r1 = arg1.resolve(tt).lift
    val r2 = arg2.resolve(tt).lift

    Unlift({
      row =>
        val z1Option = r1.apply(row)
        val z2Option = r2.apply(row)

        if (z1Option.isEmpty || z2Option.isEmpty) None
        else {
          val map: ListMap[T1, T2] = ListMap(z1Option.get.toSeq.zip(
            z2Option.get.toSeq
          ): _*)

          Some(map)
        }
    })
  }
}