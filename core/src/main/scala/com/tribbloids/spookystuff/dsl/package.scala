package com.tribbloids.spookystuff

import java.util.Date

import com.tribbloids.spookystuff.actions._
import com.tribbloids.spookystuff.doc.{Doc, Elements, PageUID, Unstructured}
import com.tribbloids.spookystuff.expressions._
import com.tribbloids.spookystuff.rdd.FetchedDataset
import com.tribbloids.spookystuff.row.{FetchedRow, Field, SquashedFetchedRDD}
import com.tribbloids.spookystuff.utils.Default
import org.apache.spark.rdd.RDD

import scala.collection.immutable.ListMap
import scala.collection.{GenTraversableOnce, IterableLike}
import scala.language.implicitConversions
import scala.reflect.ClassTag

package object dsl {
  //  type SerializableCookie = Cookie with Serializable

  implicit def PageRowRDDToRDD(wrapper: FetchedDataset): SquashedFetchedRDD = wrapper.rdd

  implicit def spookyContextToPageRowRDD(spooky: SpookyContext): FetchedDataset = spooky.blankFetchedDataset

  implicit def traceView(trace: Trace): TraceView = new TraceView(trace)

  implicit def traceSetView[Repr](traces: Repr)(implicit f: Repr => Set[Trace]): TraceSetView = new TraceSetView(traces)

  implicit def actionToTraceSet(action: Action): Set[Trace] = Set(List(action))

  //------------------------------------------------------------

  //this hierarchy aims to create a short DSL for selecting components from PageRow, e.g.:
  //'abc:  cells with key "abc", tempkey precedes ordinary key
  //'abc.S("div#a1"): all children of an unstructured field (either a page or element) that match the selector
  //S("div#a1"): all children of the only page that match the selector, if multiple page per row, throws an exception
  //$_*("div#a1"): all children of all pages that match the selector.
  //'abc.S("div#a1").head: first child of an unstructured field (either a page or element) that match the selector
  //'abc.S("div#a1").text: first text of an unstructured field that match the selector
  //'abc.S("div#a1").texts: all texts of an unstructured field that match the selector
  //'abc.S("div#a1").attr("src"): first "src" attribute of an unstructured field that match the selector
  //'abc.S("div#a1").attrs("src"): first "src" attribute of an unstructured field that match the selector

  def S(selector: String): FindAllExpr = GetOnlyPageExpr.findAll(selector)
  def S(selector: String, i: Int): Extraction[Unstructured] = {
    val expr = GetOnlyPageExpr.findAll(selector)
    new IterableLikeExprView(expr).get(i)
  }
  def S = GetOnlyPageExpr
  def S_*(selector: String): FindAllExpr = GetAllPagesExpr.findAll(selector)
  def S_*(selector: String, i: Int): Extraction[Unstructured] = {
    val expr = GetAllPagesExpr.findAll(selector)
    new IterableLikeExprView(expr).get(i)
  }
  def `S_*` = GetAllPagesExpr

  def G = GroupIndexExpr

  def A(selector: String): FindAllExpr = 'A.findAll(selector)
  def A(selector: String, i: Int): Extraction[Unstructured] = {
    val expr = 'A.findAll(selector)
    new IterableLikeExprView(expr).get(i)
  }

  implicit def symbolToField(symbol: Symbol): Field = Option(symbol).map(v => Field(v.name)).orNull

  // Unlike ExpressionLike, this wrapper is classTagged.
  implicit class ExprView[+R: ClassTag](self: Extraction[R]) extends Serializable {

    import self._

    private def defaultVal: R = Default.value[R]

    def toStr = andThen(_.toString)

    def into(field: Field) = AppendExpr.create[R](field, self)
    def ~+(field: Field) = into(field)

    def orNull[B >: R]: Extraction[B] = orElse[FetchedRow, B] {
      case _ => null.asInstanceOf[B]
    }

    def orDefault[B >: R]: Extraction[B] = orElse[FetchedRow, B] {
      case _ => defaultVal: B
    }

    def ->[B](another: Extraction[B]): Extraction[(R, B)] = {
      val lifted = {
        row: FetchedRow =>
          if (!isDefinedAt(row) || !another.isDefinedAt(row)) None
          else Some(apply(row) -> another(row))
      }
      Function.unlift(lifted)
    }
  }

  implicit def exprToExprView[Repr](expr: Repr)(implicit f: Repr => Extraction[Any]): ExprView[Any] = f(expr)

  implicit class UnstructuredExprView(self: Extraction[Unstructured]) extends Serializable {

    def uri: Extraction[String] = self.andThen(_.uri)

    def findFirst(selector: String): FindFirstExpr = new FindFirstExpr(selector, self)

    def findAll(selector: String): FindAllExpr = new FindAllExpr(selector, self)

    def \\(selector: String) = findAll(selector)

    def child(selector: String): ChildExpr = new ChildExpr(selector, self)

    def children(selector: String): ChildrenExpr = new ChildrenExpr(selector, self)

    def \(selector: String) = children(selector)

    def text: Extraction[String] = self.andOptional(_.text)

    def code = self.andOptional(_.code)

    def formattedCode = self.andOptional(_.formattedCode)

    def ownText: Extraction[String] = self.andOptional(_.ownText)

    def allAttr: Extraction[Map[String, String]] =
      self.andOptional(_.allAttr)

    def attr(attrKey: String, noEmpty: Boolean = true): Extraction[String] =
      self.andOptional(_.attr(attrKey, noEmpty))

    def href = self.andOptional(_.href)

    def src = self.andOptional(_.src)

    def boilerPipe = self.andOptional(_.boilerPipe)
  }

  implicit class ElementsExprView(self: Extraction[Elements[_]]) extends Serializable {

    def uris: Extraction[Seq[String]] = self.andThen(_.uris)

    def texts: Extraction[Seq[String]] = self.andThen(_.texts)

    def codes: Extraction[Seq[String]] = self.andThen(_.codes)

    def ownTexts: Extraction[Seq[String]] = self.andThen(_.ownTexts)

    def allAttrs: Extraction[Seq[Map[String, String]]] =
      self.andThen(_.allAttrs)

    def attrs(attrKey: String, noEmpty: Boolean = true): Extraction[Seq[String]] =
      self.andThen(_.attrs(attrKey, noEmpty))

    def hrefs = self.andThen(_.hrefs)

    def srcs = self.andThen(_.srcs)

    def boilerPipes = self.andThen(_.boilerPipes)
  }

  implicit class PageExprView(self: Extraction[Doc]) extends Serializable {

    def uid: Extraction[PageUID] = self.andThen(_.uid)

    def contentType: Extraction[String] = self.andThen(_.contentType)

    def content: Extraction[Seq[Byte]] = self.andThen(_.content.toSeq)

    def timestamp: Extraction[Date] = self.andThen(_.timestamp)

    def saved: Extraction[Set[String]] = self.andThen(_.saved.toSet)

    def mimeType: Extraction[String] = self.andThen(_.mimeType)

    def charSet: Extraction[String] = self.andOptional(_.charset)

    def exts: Extraction[Seq[String]] = self.andThen(_.exts.toSeq)

    def defaultExt: Extraction[String] = self.andOptional(_.defaultExt)
  }

  //  implicit class PageTraversableOnceExprView(self: Expression[TraversableOnce[Page]]) extends Serializable {
  //
  //    def timestamps: Expression[Seq[Date]] = self.andMap(_.toSeq.map(_.timestamp), "timestamps")
  //
  //    def saveds: Expression[Seq[ListSet[String]]] = self.andMap(_.toSeq.map(_.saved), "saveds")
  //  }

  implicit class IterableLikeExprView[T: ClassTag, Repr](self: Extraction[IterableLike[T, Repr]]) extends Serializable {

    def head: Extraction[T] = self.andOptional(_.headOption)

    def last: Extraction[T] = self.andOptional(_.lastOption)

    def get(i: Int): Extraction[T] = self.andOptional({
      iterable =>
        val realIdx = if (i >= 0) i
        else iterable.size - i

        if (realIdx>=iterable.size || realIdx<0) None
        else Some(iterable.toSeq.apply(realIdx))
    })

    def size: Extraction[Int] = self.andThen(_.size)

    def isEmpty: Extraction[Boolean] = self.andThen(_.isEmpty)

    def nonEmpty: Extraction[Boolean] = self.andThen(_.nonEmpty)

    def mkString(sep: String = ""): Extraction[String] = self.andThen(_.mkString(sep))

    def mkString(start: String, sep: String, end: String): Extraction[String] = self.andThen(_.mkString(start, sep, end))

    //TODO: Why IterableExprView.filter cannot be applied on ZippedExpr? is the scala compiler malfunctioning?
    def zipWithKeys(keys: Extraction[Any]): ZippedExpr[Any, T] =
      new ZippedExpr[Any,T](keys.typed[IterableLike[_,_]], self)

    def zipWithValues(values: Extraction[Any]): ZippedExpr[T, Any] =
      new ZippedExpr[T,Any](self, values.typed[IterableLike[_,_]])

    def groupBy[K](f: T => K): Extraction[Map[K, Repr]] = self.andThen (
      v => v.groupBy(f)
    )

    def slice(from: Int = Int.MinValue, until: Int = Int.MaxValue): Extraction[Repr] = self.andThen (
      v => v.slice(from, until)
    )

    def filter(f: T => Boolean): Extraction[Repr] = self.andThen(_.filter(f))

    def distinct: Extraction[Seq[T]] = self.andThen(_.toSeq.distinct)

    def distinctBy[K](f: T => K): Extraction[Iterable[T]] = this.groupBy(f).andThen(
      v =>
        v.values.flatMap{
          case repr: Traversable[T] => repr.headOption
          case repr: T => Some(repr)
          case _ => None
        }
    )

    //TODO: these will cause unserializable exception, fix it!
    //    def map[B, That](f: T => B)(implicit bf: CanBuildFrom[Repr, B, That]): Expression[That] = self.andMap (
    //      v => {
    //        val vv: IterableLike[T, Repr] = v
    //        vv.map[B, That](f)(Serializable(bf))
    //      },
    //      s"map($f)"
    //    )
    //    def flatMap[B, That](f: T => GenTraversableOnce[B])(implicit bf: CanBuildFrom[Repr, B, That]): Expression[That] = self.andMap (
    //      v => v.flatMap[B, That](f)(Serializable(bf)),
    //      s"flatMap($f)"
    //    )

    def map[B](f: T => B): Extraction[Seq[B]] = self.andThen (
      v => v.toSeq.map(f)
    )

    def flatMap[B](f: T => GenTraversableOnce[B]): Extraction[Seq[B]] = self.andThen (
      v => v.toSeq.flatMap(f)
    )
  }

  implicit class StringExprView(self: Extraction[String]) extends Serializable {

    def replaceAll(regex: String, replacement: String): Extraction[String] =
      self.andThen(_.replaceAll(regex, replacement))

    def trim: Extraction[String] = self.andThen(_.trim)

    def +(another: Extraction[Any]): Extraction[String] = x"$self$another"
  }

  //--------------------------------------------------

  //TODO: clean it up
  def dynamic[T](expr: Extraction[T]): Extraction[T] = expr

  implicit def symbolToExpr(symbol: Symbol): GetExpr =
    new GetExpr(symbol.name)

  implicit def symbolToUnstructuredExprView(symbol: Symbol): UnstructuredExprView =
    new GetUnstructuredExpr(symbol.name)

  implicit def symbolToPageExprView(symbol: Symbol): PageExprView =
    new GetPageExpr(symbol.name)

  implicit def symbolToIterableLikeExprView(symbol: Symbol): IterableLikeExprView[Any, Seq[Any]] =
    new GetSeqExpr(symbol.name)

  implicit def stringToExpr(str: String): Extraction[String] = {

    val delimiter = Const.keyDelimiter
    val regex = (delimiter+"\\{[^\\{\\}\r\n]*\\}").r

    if (regex.findFirstIn(str).isEmpty)
      new Literal[String](str)
    else
      new ReplaceKeyExpr(str)
  }

  implicit def fn2GenExpression[T, R](self: T => R): ExpressionLike[T, R] = ExpressionLike.fn2GenExpression(self)

  implicit class StringRDDView(val self: RDD[String]) {

    //csv has to be headerless, there is no better solution as header will be shuffled to nowhere
    def csvToMap(headerRow: String, splitter: String = ","): RDD[Map[String,String]] = {
      val headers = headerRow.split(splitter)

      //cannot handle when a row is identical to headerline, but whatever
      self.map {
        str => {
          val values = str.split(splitter)

          ListMap(headers.zip(values): _*)
        }
      }
    }

    def tsvToMap(headerRow: String) = csvToMap(headerRow,"\t")
  }

  implicit class StrContextHelper(val strC: StringContext) extends Serializable {

    def x(fs: (Extraction[Any])*) = new InterpolateExpr(strC.parts, fs)

    def CSS() = GetOnlyPageExpr.findAll(strC.s())
    def S() = CSS()

    def CSS_*() = GetAllPagesExpr.findAll(strC.s())
    def S_*() = CSS_*()

    def A() = 'A.findAll(strC.s())
  }
}