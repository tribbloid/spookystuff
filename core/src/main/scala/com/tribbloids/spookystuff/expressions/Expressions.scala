package com.tribbloids.spookystuff.expressions

import com.tribbloids.spookystuff.doc._
import com.tribbloids.spookystuff.row.{DataRow, Field, FetchedRow}
import com.tribbloids.spookystuff.utils.Implicits._
import com.tribbloids.spookystuff.utils.Utils

import scala.collection.immutable.ListMap
import scala.collection.{IterableLike, TraversableOnce}
import scala.reflect.ClassTag

//just a simple wrapper for T, this is the only way to execute a action
//this is the only serializable LiftedExpression that can be shipped remotely
final case class Literal[+T: ClassTag](value: T) extends LiftedExpression[T] {

  def liftApply(v1: FetchedRow): Option[T] = Some(value)
}

case object NullLiteral extends LiftedExpression[Null]{

  def liftApply(v1: FetchedRow): Option[Null] = None
}

class GetExpr(val field: Field) extends LiftedExpression[Any] {

  def liftApply(v1: FetchedRow): Option[Any] = {

    v1.dataRow.get(field)
      .orElse(v1.dataRow.get(field.copy(isWeak = true)))
  }
}

object GroupIndexExpr extends LiftedExpression[Int] {

  def liftApply(v1: FetchedRow): Option[Int] = Some(v1.dataRow.groupIndex)
}

class GetUnstructuredExpr(val field: Field) extends LiftedExpression[Unstructured] {

  def liftApply(v1: FetchedRow): Option[Unstructured] = {
    v1.getUnstructured(field)
      .orElse(v1.getUnstructured(field.copy(isWeak = true)))
  }
}

class GetPageExpr(val field: Field) extends LiftedExpression[Doc] {

  def liftApply(v1: FetchedRow): Option[Doc] = v1.getPage(field.name)
}

class FindFirstExpr(selector: String, param: Expression[Unstructured]) extends LiftedExpression[Unstructured] {

  def liftApply(v1: FetchedRow): Option[Unstructured] = param.lift(v1).flatMap(_.findFirst(selector))

  def expand(range: Range) = new LiftedExpression[Siblings[Unstructured]] {

    def liftApply(v1: FetchedRow) = param.lift(v1).flatMap(_.findFirstWithSiblings(selector, range))
  }
}

class FindAllExpr(selector: String, param: Expression[Unstructured]) extends LiftedExpression[Elements[Unstructured]] {

  def liftApply(v1: FetchedRow): Option[Elements[Unstructured]] = param.lift(v1).map(_.findAll(selector))

  def expand(range: Range) = new LiftedExpression[Elements[Siblings[Unstructured]]] {

    def liftApply(v1: FetchedRow) = param.lift(v1).map(_.findAllWithSiblings(selector, range))
  }
}

class ChildExpr(selector: String, param: Expression[Unstructured]) extends LiftedExpression[Unstructured] {

  def liftApply(v1: FetchedRow): Option[Unstructured] = param.lift(v1).flatMap(_.child(selector))

  def expand(range: Range) = new LiftedExpression[Siblings[Unstructured]] {

    def liftApply(v1: FetchedRow) = param.lift(v1).flatMap(_.childWithSiblings(selector, range))
  }
}

class ChildrenExpr(selector: String, param: Expression[Unstructured]) extends LiftedExpression[Elements[Unstructured]] {

  def liftApply(v1: FetchedRow): Option[Elements[Unstructured]] = param.lift(v1).map(_.children(selector))

  def expand(range: Range) = new LiftedExpression[Elements[Siblings[Unstructured]]] {

    def liftApply(v1: FetchedRow) = param.lift(v1).map(_.childrenWithSiblings(selector, range))
  }
}

class GetSeqExpr(val field: Field) extends LiftedExpression[Seq[Any]] {

  def liftApply(v1: FetchedRow): Option[Seq[Any]] = v1.dataRow.get(field).flatMap {
    case v: TraversableOnce[Any] => Some(v.toSeq)
    case v: Array[Any] => Some(v)
    case _ => None
  }
}

object GetOnlyPageExpr extends LiftedExpression[Doc] {

  def liftApply(v1: FetchedRow): Option[Doc] = v1.getOnlyPage
}

object GetAllPagesExpr extends LiftedExpression[Elements[Doc]] {

  def liftApply(v1: FetchedRow): Option[Elements[Doc]] = Some(new Elements(v1.pages.toList))
}

class ReplaceKeyExpr(str: String) extends LiftedExpression[String] {

  def liftApply(v1: FetchedRow): Option[String] = v1.dataRow.replaceInto(str)
}

class InterpolateExpr(parts: Seq[String], fs: Seq[Expression[Any]]) extends LiftedExpression[String] {

  if (parts.length != fs.length + 1)
    throw new IllegalArgumentException("wrong number of arguments for interpolated string")

  def liftApply(v1: FetchedRow): Option[String] = {

    val iParts = parts.map(v1.dataRow.replaceInto(_))
    val iFs = fs.map(_.lift.apply(v1))

    val result = if (iParts.contains(None) || iFs.contains(None)) None
    else Some(iParts.zip(iFs).map(tpl => tpl._1.get + tpl._2.get).mkString + iParts.last.get)

    result
  }
}

class ZippedExpr[T1,+T2](param1: Expression[IterableLike[T1, _]], param2: Expression[IterableLike[T2, _]]) extends LiftedExpression[Map[T1, T2]] {

  def liftApply(v1: FetchedRow): Option[Map[T1, T2]] = {

    val z1Option = param1.lift(v1)
    val z2Option = param2.lift(v1)

    if (z1Option.isEmpty || z2Option.isEmpty) return None

    val map: ListMap[T1, T2] = ListMap(z1Option.get.toSeq.zip(z2Option.get.toSeq): _*)

    Some(map)
  }
}

object AppendExpr {

  def apply[T: ClassTag](
                          field: Field,
                          expr: Expression[T]
                        ): AppendExpr[T] = {

    val effectiveField = field.!

    new AppendExpr[T](effectiveField, expr)
  }
}

class AppendExpr[+T: ClassTag] private(
                                        override val field: Field,
                                        expr: Expression[T]
                                      ) extends NamedExpr[Seq[T]] {

  override def isDefinedAt(x: (DataRow, Seq[Fetched])): Boolean = true

  override def apply(v1: (DataRow, Seq[Fetched])): Seq[T] = {

    val lastOption = expr.lift(v1)
    val oldOption = v1.dataRow.get(field)

    oldOption.toSeq.flatMap{
      old =>
        Utils.asIterable(old)
    } ++ lastOption
  }
}