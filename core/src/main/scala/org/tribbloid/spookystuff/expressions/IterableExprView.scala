package org.tribbloid.spookystuff.expressions

import scala.reflect.ClassTag

/**
 * Created by peng on 11/29/14.
 */
final class IterableExprView[T: ClassTag](self: Expression[Iterable[T]]) {

  import org.tribbloid.spookystuff.dsl._

  def head: Expression[T] = self.andFlatMap(_.headOption, "head")

  def last: Expression[T] = self.andFlatMap(_.lastOption, "last")

  def get(i: Int): Expression[T] = self.andFlatMap({
    iterable =>
      val realIdx = if (i >= 0) i
      else iterable.size - i

      if (realIdx>=iterable.size || realIdx<0) None
      else Some(iterable.toSeq.apply(realIdx))
  },
  s"get($i)")

  def size: Expression[Int] = self.andMap(_.size, "size")

  def mkString(sep: String): Expression[String] = self.andMap(_.mkString(sep), s"mkString($sep)")

  //TODO: Why IterableExprView.filter cannot be applied on ZippedExpr? is the scala compiler malfunctioning?
  def zipWithKeys(keys: Expression[Any]): ZippedExpr[Any, T] =
    new ZippedExpr[Any,T](keys.typed[Seq[_]], self)

  def zipWithValues(values: Expression[Any]): ZippedExpr[T, Any] =
    new ZippedExpr[T,Any](self, values.typed[Seq[_]])

  def filter(f: T => Boolean) = self.andMap(_.filter(f))

  //TODO: handle exception
  //  def only: Expr[T] =
  //    expr.andThen(NamedFunction1("only", _.map{
  //      seq =>
  //        assert(seq.size == 1)
  //        seq.head
  //    }))
}