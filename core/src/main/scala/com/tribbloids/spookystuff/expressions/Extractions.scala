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
final case class Literal[+T: ClassTag](value: T) extends Extraction[T] {

  override def isDefinedAt(x: (DataRow, Seq[Fetched])): Boolean = true

  override def apply(v1: (DataRow, Seq[Fetched])): T = value

  override def toString = "'" + value.toString + "'"
}

case object NullLiteral extends Extraction[Null]{

  override def isDefinedAt(x: (DataRow, Seq[Fetched])): Boolean = false

  override def apply(v1: (DataRow, Seq[Fetched])): Null = throw new MatchError("impossible")
}

case class GetExpr(field: Field) extends UnliftedExtr[Any] {

  def liftApply(v1: FetchedRow): Option[Any] = {

    v1.dataRow.get(field)
      .orElse(v1.dataRow.get(field.copy(isWeak = true)))
  }
}

case object GroupIndexExpr extends UnliftedExtr[Int] {

  def liftApply(v1: FetchedRow): Option[Int] = Some(v1.dataRow.groupIndex)
}

case class GetUnstructuredExpr(field: Field) extends UnliftedExtr[Unstructured] {

  def liftApply(v1: FetchedRow): Option[Unstructured] = {
    v1.getUnstructured(field)
      .orElse(v1.getUnstructured(field.copy(isWeak = true)))
  }
}

case class GetPageExpr(field: Field) extends UnliftedExtr[Doc] {

  def liftApply(v1: FetchedRow): Option[Doc] = v1.getPage(field.name)
}

case class FindFirstExpr(selector: String, param: Extraction[Unstructured]) extends UnliftedExtr[Unstructured] {

  def liftApply(v1: FetchedRow): Option[Unstructured] = param.lift(v1).flatMap(_.findFirst(selector))

  case class expand(range: Range) extends UnliftedExtr[Siblings[Unstructured]] {

    def liftApply(v1: FetchedRow) = param.lift(v1).flatMap(_.findFirstWithSiblings(selector, range))
  }
}

case class FindAllExpr(selector: String, param: Extraction[Unstructured]) extends UnliftedExtr[Elements[Unstructured]] {

  def liftApply(v1: FetchedRow): Option[Elements[Unstructured]] = param.lift(v1).map(_.findAll(selector))

  case class expand(range: Range) extends UnliftedExtr[Elements[Siblings[Unstructured]]] {

    def liftApply(v1: FetchedRow) = param.lift(v1).map(_.findAllWithSiblings(selector, range))
  }
}

case class ChildExpr(selector: String, param: Extraction[Unstructured]) extends UnliftedExtr[Unstructured] {

  def liftApply(v1: FetchedRow): Option[Unstructured] = param.lift(v1).flatMap(_.child(selector))

  case class expand(range: Range) extends UnliftedExtr[Siblings[Unstructured]] {

    def liftApply(v1: FetchedRow) = param.lift(v1).flatMap(_.childWithSiblings(selector, range))
  }
}

case class ChildrenExpr(selector: String, param: Extraction[Unstructured]) extends UnliftedExtr[Elements[Unstructured]] {

  def liftApply(v1: FetchedRow): Option[Elements[Unstructured]] = param.lift(v1).map(_.children(selector))

  case class expand(range: Range) extends UnliftedExtr[Elements[Siblings[Unstructured]]] {

    def liftApply(v1: FetchedRow) = param.lift(v1).map(_.childrenWithSiblings(selector, range))
  }
}

case class GetSeqExpr(field: Field) extends UnliftedExtr[Seq[Any]] {

  def liftApply(v1: FetchedRow): Option[Seq[Any]] = v1.dataRow.get(field).flatMap {
    case v: TraversableOnce[Any] => Some(v.toSeq)
    case v: Array[Any] => Some(v)
    case _ => None
  }
}

case object GetOnlyPageExpr extends UnliftedExtr[Doc] {

  def liftApply(v1: FetchedRow): Option[Doc] = v1.getOnlyPage
}

case object GetAllPagesExpr extends UnliftedExtr[Elements[Doc]] {

  def liftApply(v1: FetchedRow): Option[Elements[Doc]] = Some(new Elements(v1.pages.toList))
}

case class ReplaceKeyExpr(str: String) extends UnliftedExtr[String] {

  def liftApply(v1: FetchedRow): Option[String] = v1.dataRow.replaceInto(str)
}

case class InterpolateExpr(parts: Seq[String], fs: Seq[Extraction[Any]]) extends UnliftedExtr[String] {

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

case class ZippedExpr[T1,+T2](param1: Extraction[IterableLike[T1, _]], param2: Extraction[IterableLike[T2, _]]) extends UnliftedExtr[Map[T1, T2]] {

  def liftApply(v1: FetchedRow): Option[Map[T1, T2]] = {

    val z1Option = param1.lift(v1)
    val z2Option = param2.lift(v1)

    if (z1Option.isEmpty || z2Option.isEmpty) return None

    val map: ListMap[T1, T2] = ListMap(z1Option.get.toSeq.zip(z2Option.get.toSeq): _*)

    Some(map)
  }
}

object AppendExpr {

  def create[T: ClassTag](
                           field: Field,
                           expr: Extraction[T]
                         ): AppendExpr[T] = {

    val effectiveField = field.!

    new AppendExpr[T](effectiveField, expr)
  }
}

case class AppendExpr[+T: ClassTag] private(
                                             override val field: Field,
                                             expr: Extraction[T]
                                           ) extends NamedExtr[Seq[T]] {

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