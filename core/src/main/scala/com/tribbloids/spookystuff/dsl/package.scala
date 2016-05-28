package com.tribbloids.spookystuff

import java.util.Date

import com.tribbloids.spookystuff.actions._
import com.tribbloids.spookystuff.doc.{Doc, Elements, PageUID, Unstructured}
import com.tribbloids.spookystuff.extractors._
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
  def S(selector: String, i: Int): Extractor[Unstructured] = {
    val expr = GetOnlyPageExpr.findAll(selector)
    new IterableLikeExprView(expr).get(i)
  }
  def S = GetOnlyPageExpr
  def S_*(selector: String): FindAllExpr = GetAllPagesExpr.findAll(selector)
  def S_*(selector: String, i: Int): Extractor[Unstructured] = {
    val expr = GetAllPagesExpr.findAll(selector)
    new IterableLikeExprView(expr).get(i)
  }
  def `S_*` = GetAllPagesExpr

  def G = GroupIndexExpr

  def A(selector: String): FindAllExpr = 'A.findAll(selector)
  def A(selector: String, i: Int): Extractor[Unstructured] = {
    val expr = 'A.findAll(selector)
    new IterableLikeExprView(expr).get(i)
  }

  implicit def symbolToField(symbol: Symbol): Field = Option(symbol).map(v => Field(v.name)).orNull

  // Unlike ExpressionLike, this wrapper is classTagged.
  implicit class ExprView[+R: ClassTag](self: Extractor[R]) extends Serializable {

    import self._

    private def defaultVal: R = Default.value[R]

    def toStr = andThen(_.toString)

    def into(field: Field) = AppendExpr.create[R](field, self)
    def ~+(field: Field) = into(field)

    def orNull[B >: R]: Extractor[B] = orElse[FetchedRow, B] {
      case _ => null.asInstanceOf[B]
    }

    def orDefault[B >: R]: Extractor[B] = orElse[FetchedRow, B] {
      case _ => defaultVal: B
    }

    def ->[B](another: Extractor[B]): Extractor[(R, B)] = {
      val lifted = {
        row: FetchedRow =>
          if (!isDefinedAt(row) || !another.isDefinedAt(row)) None
          else Some(apply(row) -> another(row))
      }
      Function.unlift(lifted)
    }
  }

  implicit def exprToExprView[Repr](expr: Repr)(implicit f: Repr => Extractor[Any]): ExprView[Any] = f(expr)

  implicit class UnstructuredExprView(self: Extractor[Unstructured]) extends Serializable {

    def uri: Extractor[String] = self.andThen(_.uri)

    def findAll(selector: String): FindAllExpr = new FindAllExpr(selector, self)
    def \\(selector: String) = findAll(selector)
    def findFirst(selector: String) = findAll(selector).head

    def children(selector: String): ChildrenExpr = new ChildrenExpr(selector, self)
    def \(selector: String) = children(selector)
    def child(selector: String) = children(selector).head

    def text: Extractor[String] = self.andOptional(_.text)

    def code = self.andOptional(_.code)

    def formattedCode = self.andOptional(_.formattedCode)

    def ownText: Extractor[String] = self.andOptional(_.ownText)

    def allAttr: Extractor[Map[String, String]] =
      self.andOptional(_.allAttr)

    def attr(attrKey: String, noEmpty: Boolean = true): Extractor[String] =
      self.andOptional(_.attr(attrKey, noEmpty))

    def href = self.andOptional(_.href)

    def src = self.andOptional(_.src)

    def boilerPipe = self.andOptional(_.boilerPipe)
  }

  implicit class ElementsExprView(self: Extractor[Elements[_]]) extends Serializable {

    def uris: Extractor[Seq[String]] = self.andThen(_.uris)

    def texts: Extractor[Seq[String]] = self.andThen(_.texts)

    def codes: Extractor[Seq[String]] = self.andThen(_.codes)

    def ownTexts: Extractor[Seq[String]] = self.andThen(_.ownTexts)

    def allAttrs: Extractor[Seq[Map[String, String]]] =
      self.andThen(_.allAttrs)

    def attrs(attrKey: String, noEmpty: Boolean = true): Extractor[Seq[String]] =
      self.andThen(_.attrs(attrKey, noEmpty))

    def hrefs = self.andThen(_.hrefs)

    def srcs = self.andThen(_.srcs)

    def boilerPipes = self.andThen(_.boilerPipes)
  }

  implicit class PageExprView(self: Extractor[Doc]) extends Serializable {

    def uid: Extractor[PageUID] = self.andThen(_.uid)

    def contentType: Extractor[String] = self.andThen(_.contentType)

    def content: Extractor[Seq[Byte]] = self.andThen(_.content.toSeq)

    def timestamp: Extractor[Date] = self.andThen(_.timestamp)

    def saved: Extractor[Set[String]] = self.andThen(_.saved.toSet)

    def mimeType: Extractor[String] = self.andThen(_.mimeType)

    def charSet: Extractor[String] = self.andOptional(_.charset)

    def exts: Extractor[Seq[String]] = self.andThen(_.exts.toSeq)

    def defaultExt: Extractor[String] = self.andOptional(_.defaultExt)
  }

  //  implicit class PageTraversableOnceExprView(self: Expression[TraversableOnce[Page]]) extends Serializable {
  //
  //    def timestamps: Expression[Seq[Date]] = self.andMap(_.toSeq.map(_.timestamp), "timestamps")
  //
  //    def saveds: Expression[Seq[ListSet[String]]] = self.andMap(_.toSeq.map(_.saved), "saveds")
  //  }

  implicit class IterableLikeExprView[T: ClassTag, Repr](self: Extractor[IterableLike[T, Repr]]) extends Serializable {

    def head: Extractor[T] = self.andOptional(_.headOption)

    def last: Extractor[T] = self.andOptional(_.lastOption)

    def get(i: Int): Extractor[T] = self.andOptional({
      iterable =>
        val realIdx = if (i >= 0) i
        else iterable.size - i

        if (realIdx>=iterable.size || realIdx<0) None
        else Some(iterable.toSeq.apply(realIdx))
    })

    def size: Extractor[Int] = self.andThen(_.size)

    def isEmpty: Extractor[Boolean] = self.andThen(_.isEmpty)

    def nonEmpty: Extractor[Boolean] = self.andThen(_.nonEmpty)

    def mkString(sep: String = ""): Extractor[String] = self.andThen(_.mkString(sep))

    def mkString(start: String, sep: String, end: String): Extractor[String] = self.andThen(_.mkString(start, sep, end))

    //TODO: Why IterableExprView.filter cannot be applied on ZippedExpr? is the scala compiler malfunctioning?
    def zipWithKeys(keys: Extractor[Any]): ZippedExpr[Any, T] =
      new ZippedExpr[Any,T](keys.typed[IterableLike[_,_]], self)

    def zipWithValues(values: Extractor[Any]): ZippedExpr[T, Any] =
      new ZippedExpr[T,Any](self, values.typed[IterableLike[_,_]])

    def groupBy[K](f: T => K): Extractor[Map[K, Repr]] = self.andThen (
      v => v.groupBy(f)
    )

    def slice(from: Int = Int.MinValue, until: Int = Int.MaxValue): Extractor[Repr] = self.andThen (
      v => v.slice(from, until)
    )

    def filter(f: T => Boolean): Extractor[Repr] = self.andThen(_.filter(f))

    def distinct: Extractor[Seq[T]] = self.andThen(_.toSeq.distinct)

    def distinctBy[K](f: T => K): Extractor[Iterable[T]] = this.groupBy(f).andThen(
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

    def map[B](f: T => B): Extractor[Seq[B]] = self.andThen (
      v => v.toSeq.map(f)
    )

    def flatMap[B](f: T => GenTraversableOnce[B]): Extractor[Seq[B]] = self.andThen (
      v => v.toSeq.flatMap(f)
    )
  }

  implicit class StringExprView(self: Extractor[String]) extends Serializable {

    def replaceAll(regex: String, replacement: String): Extractor[String] =
      self.andThen(_.replaceAll(regex, replacement))

    def trim: Extractor[String] = self.andThen(_.trim)

    def +(another: Extractor[Any]): Extractor[String] = x"$self$another"
  }

  //--------------------------------------------------

  //TODO: clean it up
  def dynamic[T](expr: Extractor[T]): Extractor[T] = expr

  implicit def symbolToExpr(symbol: Symbol): GetExpr =
    new GetExpr(symbol.name)

  implicit def symbolToUnstructuredExprView(symbol: Symbol): UnstructuredExprView =
    new GetUnstructuredExpr(symbol.name)

  implicit def symbolToPageExprView(symbol: Symbol): PageExprView =
    new GetPageExpr(symbol.name)

  implicit def symbolToIterableLikeExprView(symbol: Symbol): IterableLikeExprView[Any, Seq[Any]] =
    new GetSeqExpr(symbol.name)

  implicit def stringToExpr(str: String): Extractor[String] = {

    val delimiter = Const.keyDelimiter
    val regex = (delimiter+"\\{[^\\{\\}\r\n]*\\}").r

    if (regex.findFirstIn(str).isEmpty)
      new Literal[String](str)
    else
      new ReplaceKeyExpr(str)
  }

  implicit def fn2GenExpression[T, R](self: T => R): GenExtractor[T, R] = GenExtractor.fn2GenExtractor(self)

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

    def x(fs: (Extractor[Any])*) = new InterpolateExpr(strC.parts, fs)

    def CSS() = GetOnlyPageExpr.findAll(strC.s())
    def S() = CSS()

    def CSS_*() = GetAllPagesExpr.findAll(strC.s())
    def S_*() = CSS_*()

    def A() = 'A.findAll(strC.s())
  }
}