package org.tribbloid.spookystuff.expressions

import org.tribbloid.spookystuff.Const
import org.tribbloid.spookystuff.entity.PageRow
import org.tribbloid.spookystuff.pages.{Siblings, Elements, Page, Unstructured}

import scala.collection.{IterableLike, TraversableOnce}
import scala.collection.immutable.ListMap
import scala.reflect.ClassTag

//just a simple wrapper for T, this is the only way to execute a action
//this is the only Expression that can be shipped remotely
final case class Literal[+T: ClassTag](value: T) extends Expression[T] {//all select used in query cannot have name changed

  override val name = "'"+value.toString+"'" //quoted to avoid confusion with Get-ish Expr

  override def apply(v1: PageRow): Option[T] = Some(value)
}

class GetExpr(override val name: String) extends Expression[Any] {

  override def apply(v1: PageRow): Option[Any] = v1.get(name)
}

class GetUnstructuredExpr(override val name: String) extends Expression[Unstructured] {

  override def apply(v1: PageRow): Option[Unstructured] = v1.getUnstructured(name)
}

class GetPageExpr(override val name: String) extends Expression[Page] {

  override def apply(v1: PageRow): Option[Page] = v1.getPage(name)
}

class ChildExpr(selector: String, param: Expression[Unstructured]) extends Expression[Unstructured] {

  override val name = s"${param.name}.child($selector)"

  override def apply(v1: PageRow): Option[Unstructured] = param(v1).flatMap(_.child(selector))

  def expand(range: Range) = new Expression[Siblings[Unstructured]] {
    override val name: String = s"${this.name}.expand(${range.head} -> ${range.last})"

    override def apply(v1: PageRow) = param(v1).flatMap(_.childWithSiblings(selector, range))
  }
}

class ChildrenExpr(selector: String, param: Expression[Unstructured]) extends Expression[Elements[Unstructured]] {

  override val name = s"${param.name}.children($selector)"

  override def apply(v1: PageRow): Option[Elements[Unstructured]] = param(v1).map(_.children(selector))

  def expand(range: Range) = new Expression[Elements[Siblings[Unstructured]]] {
    override val name: String = s"${this.name}.expand(${range.head} -> ${range.last})"

    override def apply(v1: PageRow) = param(v1).map(_.childrenWithSiblings(selector, range))
  }
}

class GetSeqExpr(override val name: String) extends Expression[Seq[Any]] {

  override def apply(v1: PageRow): Option[Seq[Any]] = v1.get(name).flatMap {
    case v: TraversableOnce[Any] => Some(v.toSeq)
    case v: Array[Any] => Some(v)
    case _ => None
  }
}

object GetOnlyPageExpr extends Expression[Page] {
  override val name = Const.onlyPageWildcard

  override def apply(v1: PageRow): Option[Page] = v1.getOnlyPage
}

object GetAllPagesExpr extends Expression[Elements[Page]] {
  override val name = Const.allPagesWildcard

  override def apply(v1: PageRow): Option[Elements[Page]] = Some(new Elements(v1.pages.toList))
}

object GetSegmentIDExpr extends Expression[String] {
  override val name = "SegmentID"

  override def apply(v1: PageRow): Option[String] =Option(v1.segmentID.toString)
}

class ReplaceKeyExpr(str: String) extends Expression[String] {

  override val name = str

  override def apply(v1: PageRow): Option[String] = v1.replaceInto(str)
}

class InterpolateExpr(parts: Seq[String], fs: Seq[Expression[Any]])
  extends Expression[String] {

  override val name = parts.zip(fs.map(_.name)).map(tpl => tpl._1+tpl._2).mkString + parts.last

  if (parts.length != fs.length + 1)
    throw new IllegalArgumentException("wrong number of arguments for interpolated string")

  override def apply(v1: PageRow): Option[String] = {

    val iParts = parts.map(v1.replaceInto(_))
    val iFs = fs.map(_.apply(v1))

    if (iParts.contains(None) || iFs.contains(None)) None
    else Some(iParts.zip(iFs).map(tpl => tpl._1.get + tpl._2.get).mkString + iParts.last.get)
  }
}

class ZippedExpr[T1,+T2](param1: Expression[IterableLike[T1, _]], param2: Expression[IterableLike[T2, _]]) extends Expression[Map[T1, T2]] {
  override val name: String = s"${param1.name}.zip(${param2.name})"

  override def apply(v1: PageRow): Option[Map[T1, T2]] = {

    val z1Option = param1(v1)
    val z2Option = param2(v1)

    if (z1Option.isEmpty || z2Option.isEmpty) return None

    val map: ListMap[T1, T2] = ListMap(z1Option.get.toSeq.zip(z2Option.get.toSeq): _*)

    Some(map)
  }
}

class InsertIntoExpr[+T: ClassTag](override val name: String, expr: Expression[T]) extends ForceExpression[Seq[T]] {

  override def apply(v1: PageRow): Option[Seq[T]] = {
    val lastOption = expr(v1)
    val oldOption = v1.get(name)

    //in current implementation
    oldOption match {
      case Some(v: TraversableOnce[T]) => Some(v.toSeq ++ lastOption)
      case Some(v: Array[T]) => Some(v ++ lastOption)
      case Some(v: T) => Some(Seq(v) ++ lastOption)
      case None => Some(lastOption.toSeq)
      case _ => Some(lastOption.toSeq)
    }
  }
}