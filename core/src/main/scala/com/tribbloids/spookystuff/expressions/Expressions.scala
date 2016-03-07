package com.tribbloids.spookystuff.expressions

import com.tribbloids.spookystuff.Const
import com.tribbloids.spookystuff.expressions.ExpressionLike._
import com.tribbloids.spookystuff.pages.{Elements, Page, Siblings, Unstructured}
import com.tribbloids.spookystuff.row.{Field, PageRow}

import scala.collection.immutable.ListMap
import scala.collection.{IterableLike, TraversableOnce}
import scala.reflect.ClassTag

import com.tribbloids.spookystuff.utils.Views._

//just a simple wrapper for T, this is the only way to execute a action
//this is the only serializable Expression that can be shipped remotely
final case class Literal[+T: ClassTag](value: T) extends Expression[T] {

  override val field: Field  = "'"+value.toString+"'" //quoted to avoid confusion with Get-ish Expression

  override def apply(v1: PageRow): Option[T] = Some(value)
}

case object NullLiteral extends Expression[Null]{

  override val field: Field = "Null"

  override def apply(v1: PageRow): Option[Null] = None
}

class GetExpr(override val field: Field) extends Expression[Any] {

  override def apply(v1: PageRow): Option[Any] = {

    v1.dataRow.get(field)
      .orElse(v1.dataRow.get(field.copy(isWeak = true)))
  }
}

object GroupIndexExpr extends Expression[Int] {

  override val field: Field = Const.groupIndexExtractor

  override def apply(v1: PageRow): Option[Int] = Some(v1.dataRow.groupIndex)
}

class GetUnstructuredExpr(override val field: Field) extends Expression[Unstructured] {

  override def apply(v1: PageRow): Option[Unstructured] = {
    v1.getUnstructured(field)
      .orElse(v1.getUnstructured(field.copy(isWeak = true)))
  }
}

class GetPageExpr(override val field: Field) extends Expression[Page] {

  override def apply(v1: PageRow): Option[Page] = v1.getPage(name)
}

class FindFirstExpr(selector: String, param: Expression[Unstructured]) extends Expression[Unstructured] {

  override val field: Field  = s"${param.name}.findFirst($selector)"

  override def apply(v1: PageRow): Option[Unstructured] = param(v1).flatMap(_.findFirst(selector))

  def expand(range: Range) = new Expression[Siblings[Unstructured]] {
    override val field: Field = s"${FindFirstExpr.this.name}.expand(${range.head} -> ${range.last})"

    override def apply(v1: PageRow) = param(v1).flatMap(_.findFirstWithSiblings(selector, range))
  }
}

class FindAllExpr(selector: String, param: Expression[Unstructured]) extends Expression[Elements[Unstructured]] {

  override val field: Field  = s"${param.name}.findAll($selector)"

  override def apply(v1: PageRow): Option[Elements[Unstructured]] = param(v1).map(_.findAll(selector))

  def expand(range: Range) = new Expression[Elements[Siblings[Unstructured]]] {
    override val field: Field = s"${FindAllExpr.this.name}.expand(${range.head} -> ${range.last})"

    override def apply(v1: PageRow) = param(v1).map(_.findAllWithSiblings(selector, range))
  }
}

class ChildExpr(selector: String, param: Expression[Unstructured]) extends Expression[Unstructured] {

  override val field: Field  = s"${param.name}.child($selector)"

  override def apply(v1: PageRow): Option[Unstructured] = param(v1).flatMap(_.child(selector))

  def expand(range: Range) = new Expression[Siblings[Unstructured]] {
    override val field: Field = s"${ChildExpr.this.name}.expand(${range.head} -> ${range.last})"

    override def apply(v1: PageRow) = param(v1).flatMap(_.childWithSiblings(selector, range))
  }
}

class ChildrenExpr(selector: String, param: Expression[Unstructured]) extends Expression[Elements[Unstructured]] {

  override val field: Field  = s"${param.name}.children($selector)"

  override def apply(v1: PageRow): Option[Elements[Unstructured]] = param(v1).map(_.children(selector))

  def expand(range: Range) = new Expression[Elements[Siblings[Unstructured]]] {
    override val field: Field = s"${this.name}.expand(${range.head} -> ${range.last})"

    override def apply(v1: PageRow) = param(v1).map(_.childrenWithSiblings(selector, range))
  }
}

class GetSeqExpr(override val field: Field) extends Expression[Seq[Any]] {

  override def apply(v1: PageRow): Option[Seq[Any]] = v1.dataRow.get(field).flatMap {
    case v: TraversableOnce[Any] => Some(v.toSeq)
    case v: Array[Any] => Some(v)
    case _ => None
  }
}

object GetOnlyPageExpr extends Expression[Page] {
  override val field: Field  = Const.onlyPageExtractor

  override def apply(v1: PageRow): Option[Page] = v1.getOnlyPage
}

object GetAllPagesExpr extends Expression[Elements[Page]] {
  override val field: Field  = Const.allPagesExtractor

  override def apply(v1: PageRow): Option[Elements[Page]] = Some(new Elements(v1.pages.toList))
}

//object GetSegmentIDExpr extends Expression[String] {
//  override val field: Field  = "SegmentID"
//
//  override def apply(v1: PageRow): Option[String] =Option(v1.segmentID.toString)
//}

class ReplaceKeyExpr(str: String) extends Expression[String] {

  override val field: Field = str

  override def apply(v1: PageRow): Option[String] = v1.dataRow.replaceInto(str)
}

class InterpolateExpr(parts: Seq[String], fs: Seq[(PageRow => Option[Any])])
  extends Expression[String] {

  override val field: Field = parts
    .zip(fs.map{
      case v: InterpolateExpr => v.toString()
      case v @ _ => "${"+ v + "}"
    })
    .map(tpl => tpl._1+tpl._2).mkString + parts.last

  if (parts.length != fs.length + 1)
    throw new IllegalArgumentException("wrong number of arguments for interpolated string")

  override def apply(v1: PageRow): Option[String] = {

    val iParts = parts.map(v1.dataRow.replaceInto(_))
    val iFs = fs.map(_.apply(v1))

    val result = if (iParts.contains(None) || iFs.contains(None)) None
    else Some(iParts.zip(iFs).map(tpl => tpl._1.get + tpl._2.get).mkString + iParts.last.get)

    result
  }
}

class ZippedExpr[T1,+T2](param1: Expression[IterableLike[T1, _]], param2: Expression[IterableLike[T2, _]]) extends Expression[Map[T1, T2]] {
  override val field: Field = s"${param1.name}.zip(${param2.name})"

  override def apply(v1: PageRow): Option[Map[T1, T2]] = {

    val z1Option = param1(v1)
    val z2Option = param2(v1)

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
                                          ) extends Expression[Seq[T]] {

  override def apply(v1: PageRow): Option[Seq[T]] = {

    val lastOption = expr(v1)
    val oldOption = v1.dataRow.get(field)

    //TODO: use Utils implementation
    oldOption match {
      case Some(v: TraversableOnce[T]) => Some(v.toSeq ++ lastOption)
      case Some(v: Array[T]) => Some(v ++ lastOption)
      case Some(v: T) => Some(Seq(v) ++ lastOption)
      case None => Some(lastOption.toSeq)
      case _ => Some(lastOption.toSeq)
    }
  }
}