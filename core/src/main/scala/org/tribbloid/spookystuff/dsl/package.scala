package org.tribbloid.spookystuff

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.tribbloid.spookystuff.actions.{Action, Trace, TraceSetView, TraceView}
import org.tribbloid.spookystuff.entity.PageRow
import org.tribbloid.spookystuff.expressions._
import org.tribbloid.spookystuff.pages.{Elements, Page, Unstructured}
import org.tribbloid.spookystuff.sparkbinding.{DataFrameView, PageRowRDD, StringRDDView}

import scala.collection.IterableLike
import scala.language.implicitConversions
import scala.reflect.ClassTag

/**
* Created by peng on 9/18/14.
*/
package object dsl {

//  type SerializableCookie = Cookie with Serializable

  implicit def PageRowRDDToSelf(wrapper: PageRowRDD): RDD[PageRow] = wrapper.self
  
  implicit def spookyContextToPageRowRDD(spooky: SpookyContext): PageRowRDD =
    new PageRowRDD(spooky.sqlContext.sparkContext.parallelize(Seq(PageRow())), spooky = spooky.getContextForNewInput)

  implicit def traceView(trace: Trace): TraceView = new TraceView(trace)

  implicit def traceSetView(traces: Set[Trace]): TraceSetView = new TraceSetView(traces)

  implicit def actionToTraceSet(action: Action): Set[Trace] = Set(Seq(action))

  implicit def actionToTraceSetView(action: Action): TraceSetView = Set(Seq(action))

  //------------------------------------------------------------

  //this hierarchy aims to create a short DSL for selecting components from PageRow, e.g.:
  //'abc:  cells with key "abc", tempkey precedes ordinary key
  //'abc.$("div#a1"): all children of an unstructured field (either a page or element) that match the selector
  //$("div#a1"): all children of the only page that match the selector, if multiple page per row, throws an exception
  //$_*("div#a1"): all children of all pages that match the selector.
  //'abc.$("div#a1").head: first child of an unstructured field (either a page or element) that match the selector
  //'abc.$("div#a1").text: first text of an unstructured field that match the selector
  //'abc.$("div#a1").texts: all texts of an unstructured field that match the selector
  //'abc.$("div#a1").attr("src"): first "src" attribute of an unstructured field that match the selector
  //'abc.$("div#a1").attrs("src"): first "src" attribute of an unstructured field that match the selector

  def S(selector: String): ChildrenExpr = GetOnlyPageExpr.children(selector)
  def S(selector: String, i: Int): Expression[Unstructured] = GetOnlyPageExpr.children(selector).get(i)
  def S: Expression[Page] = GetOnlyPageExpr

  def S_*(selector: String): ChildrenExpr = GetAllPagesExpr.children(selector)
  def S_*(selector: String, i: Int): Expression[Unstructured] = GetAllPagesExpr.children(selector).get(i)
  def `S_*`: Expression[Elements[Page]] = GetAllPagesExpr

  def A(selector: String): ChildrenExpr = 'A.children(selector)
  def A(selector: String, i: Int): Expression[Unstructured] = 'A.children(selector).get(i)

  implicit def exprView[T: ClassTag](expr: Expression[T]): ExprView[T] =
    new ExprView(expr)

  implicit def unstructuredExprView(expr: Expression[Unstructured]): UnstructuredExprView =
    new UnstructuredExprView(expr)

  implicit def pageExprView(expr: Expression[Page]): PageExprView =
    new PageExprView(expr)

  implicit def elementsExprView(expr: Expression[Elements[_]]): ElementsExprView =
    new ElementsExprView(expr)

  implicit class IterableLikeExprView[T: ClassTag, Repr](self: Expression[IterableLike[T, Repr]]) {

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

    def isEmpty: Expression[Boolean] = self.andMap(_.isEmpty)

    def nonEmpty: Expression[Boolean] = self.andMap(_.nonEmpty)

    def mkString(sep: String = ""): Expression[String] = self.andMap(_.mkString(sep), s"mkString($sep)")

    def mkString(start: String, sep: String, end: String): Expression[String] = self.andMap(_.mkString(start, sep, end), s"mkString($sep)")

    //TODO: Why IterableExprView.filter cannot be applied on ZippedExpr? is the scala compiler malfunctioning?
    def zipWithKeys(keys: Expression[Any]): ZippedExpr[Any, T] =
      new ZippedExpr[Any,T](keys.typed[IterableLike[_,_]], self)

    def zipWithValues(values: Expression[Any]): ZippedExpr[T, Any] =
      new ZippedExpr[T,Any](self, values.typed[IterableLike[_,_]])

    def groupBy[K](f: T => K): Expression[Map[K, Repr]] = self.andMap {
      v =>
        v.groupBy(f)
    }

    def slice(from: Int = Int.MinValue, until: Int = Int.MaxValue): Expression[Repr] = self.andMap {
      v =>
        v.slice(from, until)
    }

    def filter(f: T => Boolean): Expression[Repr] = self.andMap(_.filter(f))

    def distinct: Expression[Seq[T]] = self.andMap(_.toSeq.distinct)

    def distinctBy[K](f: T => K): Expression[Iterable[T]] = this.groupBy(f).andMap{
      v =>
        v.values.flatMap{
          case repr: Traversable[T] => repr.headOption
          case repr: T => Some(repr)
          case _ => None
        }
    }

    //TODO: handle exception
    //  def only: Expr[T] =
    //    expr.andThen(NamedFunction1("only", _.map{
    //      seq =>
    //        assert(seq.size == 1)
    //        seq.head
    //    }))
  }

  implicit class StringExprView(self: Expression[String]) {

    def replaceAll(regex: String, replacement: String): Expression[String] =
      self.andMap(_.replaceAll(regex, replacement), s"replaceAll($regex,$replacement)")

    def trim: Expression[String] = self.andMap(_.trim, "trim")

    //  def urlParam
  }

  //--------------------------------------------------

  implicit def symbolToExpr(symbol: Symbol): GetExpr =
    new GetExpr(symbol.name)

  implicit def symbolToExprView(symbol: Symbol): ExprView[Any] =
    new GetExpr(symbol.name)

  implicit def symbolToUnstructuredExprView(symbol: Symbol): UnstructuredExprView =
    new GetUnstructuredExpr(symbol.name)

  implicit def symbolToPageExprView(symbol: Symbol): PageExprView =
    new GetPageExpr(symbol.name)

  implicit def symbolToIterableLikeExprView(symbol: Symbol): IterableLikeExprView[Any, Seq[Any]] =
    new GetSeqExpr(symbol.name)

  implicit def stringToExpr(str: String): Expression[String] = {

    val delimiter = Const.keyDelimiter
    val regex = (delimiter+"\\{[^\\{\\}\r\n]*\\}").r

    if (regex.findFirstIn(str).isEmpty)
      new Literal[String](str)
    else
      new ReplaceKeyExpr(str)
  }

  implicit def stringRDDToItsView(rdd: RDD[String]): StringRDDView = new StringRDDView(rdd)

  implicit def dataFrameToItsView(rdd: DataFrame): DataFrameView = new DataFrameView(rdd)

  implicit class StrContextHelper(val strC: StringContext) {

    def x(fs: Expression[Any]*) = new InterpolateExpr(strC.parts, fs)

    def CSS() = GetOnlyPageExpr.children(strC.s())
    def S() = CSS()

    def CSS_*() = GetAllPagesExpr.children(strC.s())
    def S_*() = CSS_*()

    def A() = 'A.children(strC.s())
  }
}