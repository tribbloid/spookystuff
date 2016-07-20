package com.tribbloids.spookystuff

import java.sql.Timestamp

import com.tribbloids.spookystuff.actions._
import com.tribbloids.spookystuff.doc.{Doc, DocUID, Elements, Unstructured}
import com.tribbloids.spookystuff.extractors.GenExtractor.And_->
import com.tribbloids.spookystuff.extractors._
import com.tribbloids.spookystuff.rdd.FetchedDataset
import com.tribbloids.spookystuff.row.{Field, SquashedFetchedRDD}
import com.tribbloids.spookystuff.utils.Default
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.TypeUtils
import org.apache.spark.sql.types._

import scala.collection.GenTraversableOnce
import scala.collection.immutable.ListMap
import scala.language.implicitConversions
import scala.reflect.ClassTag

package object dsl {

  type ByDoc[+R] = (Doc => R)
  type ByTrace[+R] = (Trace => R)

  implicit def PageRowRDDToRDD(wrapper: FetchedDataset): SquashedFetchedRDD = wrapper.rdd

  implicit def spookyContextToPageRowRDD(spooky: SpookyContext): FetchedDataset = spooky.createBlank

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

  import Extractors._

  def S: GenExtractor[FR, Doc] = GetOnlyPageExpr
  def S(selector: String): GenExtractor[FR, Elements[Unstructured]] = S.findAll(selector)
  //  def S(selector: String, i: Int): Extractor[Unstructured] = {
  //    val expr = this.S(selector)
  //    new IterableExprView(expr).get(i)
  //  }
  def `S_*`: GenExtractor[FR, Elements[Doc]] = GetAllPagesExpr
  def S_*(selector: String): GenExtractor[FR, Elements[Unstructured]] = `S_*`.findAll(selector)
  //  def S_*(selector: String, i: Int): Extractor[Unstructured] = {
  //    val expr = GetAllPagesExpr.findAll(selector)
  //    new IterableExprView(expr).get(i)
  //  }

  def G = GroupIndexExpr

  def A(selector: String) = 'A.findAll(selector)
  def A(selector: String, i: Int): Extractor[Unstructured] = {
    val expr = 'A.findAll(selector)
    new IterableExprView(expr).get(i)
  }

  implicit def symbolToField(symbol: Symbol): Field = Option(symbol).map(v => Field(v.name)).orNull

  // TODO: merge into GenExtraction?
  implicit class ExprView[R: ClassTag](self: Extractor[R]) extends Serializable {

    private def defaultVal: R = Default.value[R]

    def into(field: Field) = AppendExpr.create[R](field, self)
    def ~+(field: Field) = into(field)

    def -->[R2](g: Extractor[R2]) = And_->(self, g)
    //    def orNull[B >: R]: Extractor[B] = orElse[FetchedRow, B] {
    //      case _ => null.asInstanceOf[B]
    //    }
    //    def orDefault[B >: R]: Extractor[B] = orElse[FetchedRow, B] {
    //      case _ => defaultVal: B
    //    }

    //    def orNull: Extractor[R] = orElse[FetchedRow, R] {
    //      case _ => null.asInstanceOf[R]
    //    }
    //    def orDefault: Extractor[R] = orElse[FetchedRow, R] {
    //      case _ => defaultVal: R
    //    }
  }

  implicit def exprToExprView[Repr](expr: Repr)(implicit f: Repr => Extractor[Any]): ExprView[Any] = f(expr)

  implicit class UnstructuredExprView(self: Extractor[Unstructured]) extends Serializable {

    def uri: Extractor[String] = self.andThen(_.uri)

    def findAll(selector: String) = FindAllExpr(self, selector)
    def \\(selector: String) = findAll(selector)
    def findFirst(selector: String) = findAll(selector).head

    def children(selector: String) = ChildrenExpr(self, selector)
    def \(selector: String) = children(selector)
    def child(selector: String) = children(selector).head

    def text: Extractor[String] = self.andOptionFn(_.text)

    def code = self.andOptionFn(_.code)

    def formattedCode = self.andOptionFn(_.formattedCode)

    def ownText: Extractor[String] = self.andOptionFn(_.ownText)

    def allAttr: Extractor[Map[String, String]] =
      self.andOptionFn(_.allAttr)

    def attr(attrKey: String, noEmpty: Boolean = true): Extractor[String] =
      self.andOptionFn(_.attr(attrKey, noEmpty))

    def href = self.andOptionFn(_.href)

    def src = self.andOptionFn(_.src)

    def boilerPipe = self.andOptionFn(_.boilerPipe)

    def expand(range: Range) = ExpandExpr(self, range)
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

    def uid: Extractor[DocUID] = self.andThen(_.uid)

    def contentType: Extractor[String] = self.andThen(_.contentType)

    def content: Extractor[Seq[Byte]] = self.andThen(_.content.toSeq)

    def timestamp: Extractor[Timestamp] = self.andThen(_.timestamp)

    def saved: Extractor[Set[String]] = self.andThen(_.saved.toSet)

    def mimeType: Extractor[String] = self.andThen(_.mimeType)

    def charSet: Extractor[String] = self.andOptionFn(_.charset)

    def fileExtensions: Extractor[Seq[String]] = self.andThen(_.fileExtensions.toSeq)

    def defaultFileExtension: Extractor[String] = self.andOptionFn(_.defaultFileExtension)
  }

  implicit class IterableExprView[T: ClassTag](self: Extractor[Iterable[T]]) extends Serializable {

    val unboxType: DataType => DataType = {
      case ArrayType(boxed, _) => boxed
    }

    //    def andSelfType[R <: Iterable[T]](f: Iterable[T] => Option[R]) = self.andOptionFnTyped[R, Iterable[T]](
    //      f, {
    //        t => t
    //      }
    //    )
    //
    //    def andUnboxedType(f: Iterable[T] => Option[T]) = self.andOptionFnTyped[T, Iterable[T]](
    //      f, {
    //        case ArrayType(boxed, _) => boxed
    //      }
    //    )

    def head: Extractor[T] = self.andOptionTyped((v: Iterable[T]) => v.headOption, unboxType)

    def last: Extractor[T] = self.andOptionTyped((v: Iterable[T]) => v.lastOption, unboxType)

    def get(i: Int): Extractor[T] = self.andOptionTyped({
      (v: Iterable[T]) =>
        val realIdx = if (i >= 0) i
        else v.size - i

        if (realIdx>=v.size || realIdx<0) None
        else Some(v.toSeq.apply(realIdx))
    }, unboxType)

    def size: Extractor[Int] = self.andThen(_.size)

    def isEmpty: Extractor[Boolean] = self.andThen(_.isEmpty)

    def nonEmpty: Extractor[Boolean] = self.andThen(_.nonEmpty)

    def mkString(sep: String = ""): Extractor[String] = self.andThen(_.mkString(sep))

    def mkString(start: String, sep: String, end: String): Extractor[String] = self.andThen(_.mkString(start, sep, end))

    //TODO: Why IterableExprView.filter cannot be applied on ZippedExpr? is the scala compiler malfunctioning?
    def zipWithKeys(keys: Extractor[Any]): ZippedExpr[Any, T] =
      new ZippedExpr[Any,T](keys.typed[Iterable[_]], self)

    def zipWithValues(values: Extractor[Any]): ZippedExpr[T, Any] =
      new ZippedExpr[T,Any](self, values.typed[Iterable[_]])

    protected def groupByFn[K](f: T => K): (Iterable[T]) => Map[K, Iterable[T]] =
      (v: Iterable[T]) => v.groupBy(f)

    def groupBy[K: TypeTag](f: T => K): Extractor[Map[K, Iterable[T]]] = {

      val keyType = TypeUtils.catalystTypeFor[K]

      self.andTyped (
        groupByFn(f),
        {
          t =>
            MapType(keyType, t)
        }
      )
    }

    def slice(from: Int = Int.MinValue, until: Int = Int.MaxValue): Extractor[Iterable[T]] = self.andOptionTyped (
      (v: Iterable[T]) => Some(v.slice(from, until)), identity
    )

    def filter(f: T => Boolean): Extractor[Iterable[T]] = self.andOptionTyped ((v: Iterable[T]) => Some(v.filter(f)), identity)

    def distinct: Extractor[Seq[T]] = self.andOptionTyped ((v: Iterable[T]) => Some(v.toSeq.distinct), identity)

    def distinctBy[K](f: T => K): Extractor[Iterable[T]] = {
      self.andTyped (
        groupByFn(f)
          .andThen(
            v =>
              v.values.flatMap{
                case repr: Traversable[T] => repr.headOption
                case _ => None //TODO: what's the point of this? removed
              }
          ),
        identity
      )
    }

    def map[B: TypeTag](f: T => B): Extractor[Seq[B]] = self.andThen (
      v => v.toSeq.map(f)
    )

    def flatMap[B: TypeTag](f: T => GenTraversableOnce[B]): Extractor[Seq[B]] = self.andThen (
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
    GetUnstructuredExpr(symbol.name)

  implicit def symbolToPageExprView(symbol: Symbol): PageExprView =
    GetPageExpr(symbol.name)

  implicit def symbolToIterableLikeExprView(symbol: Symbol): IterableExprView[Any] =
    GetExpr(symbol.name).GetSeqExpr

  implicit def stringToExpr(str: String): Extractor[String] = {

    val delimiter = Const.keyDelimiter
    val regex = (delimiter+"\\{[^\\{\\}\r\n]*\\}").r

    if (regex.findFirstIn(str).isEmpty)
      Literal[String](str)
    else
      ReplaceKeyExpr(str)
  }

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