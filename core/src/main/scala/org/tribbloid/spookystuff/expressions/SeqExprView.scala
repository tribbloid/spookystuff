package org.tribbloid.spookystuff.expressions

import scala.reflect.ClassTag

/**
 * Created by peng on 11/29/14.
 */
class SeqExprView[T: ClassTag](self: Expression[Seq[T]]) {

  import org.tribbloid.spookystuff.dsl._

  def head: Expression[T] = self.andFlatMap(_.headOption, "head")

  def last: Expression[T] = self.andFlatMap(_.lastOption, "last")

  def get(i: Int): Expression[T] = self.andFlatMap({
    seq =>
      val realIdx = if (i >= 0) i
      else seq.size - i

      if (realIdx>=seq.size || realIdx<0) None
      else Some(seq(realIdx))
  },
  s"get($i)")

  def size: Expression[Int] = self.andMap(_.size, "size")

  def mkString(sep: String): Expression[String] = self.andMap(_.mkString(sep), s"mkString($sep)")

  def zipWithKeys(keys: Expression[Any]): ZippedExpr[Any, T] =
    new ZippedExpr[Any,T](keys.filterByType[Seq[Any]], self)

  def zipWithValues(values: Expression[Any]): ZippedExpr[T, Any] =
    new ZippedExpr[T,Any](self, values.filterByType[Seq[Any]])

  //TODO: handle exception
  //  def only: Expr[T] =
  //    expr.andThen(NamedFunction1("only", _.map{
  //      seq =>
  //        assert(seq.size == 1)
  //        seq.head
  //    }))
}