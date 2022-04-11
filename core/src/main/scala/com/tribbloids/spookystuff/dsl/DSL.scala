package com.tribbloids.spookystuff.dsl

import java.sql.Timestamp
import com.tribbloids.spookystuff.SpookyContext
import com.tribbloids.spookystuff.actions.{Action, Trace, TraceSetView, TraceView}
import com.tribbloids.spookystuff.doc.{Doc, DocUID, Elements, HasSeq, Unstructured}
import com.tribbloids.spookystuff.extractors.GenExtractor.AndThen
import com.tribbloids.spookystuff.extractors.impl.Extractors._
import com.tribbloids.spookystuff.extractors._
import com.tribbloids.spookystuff.extractors.impl.{Append, Get, Interpolate, Zipped}
import com.tribbloids.spookystuff.rdd.FetchedDataset
import com.tribbloids.spookystuff.row.{FetchedRow, Field}
import com.tribbloids.spookystuff.utils.Default
import org.apache.spark.ml.dsl.utils.refl.UnreifiedObjectType
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.MapType

import scala.collection.GenTraversableOnce
import scala.language.implicitConversions
import scala.reflect.ClassTag

/**
  * this hierarchy aims to create a short DSL for selecting components from PageRow, e.g.:
  * 'abc:  cells with key "abc", tempkey precedes ordinary key
  * 'abc.S("div#a1"): all children of an unstructured field (either a page or element) that match the selector
  * S("div#a1"): all children of the only page that match the selector, if multiple page per row, throws an exception
  * S_*("div#a1"): all children of all pages that match the selector.
  * 'abc.S("div#a1").head: first child of an unstructured field (either a page or element) that match the selector
  * 'abc.S("div#a1").text: first text of an unstructured field that match the selector
  * 'abc.S("div#a1").texts: all texts of an unstructured field that match the selector
  * 'abc.S("div#a1").attr("src"): first "src" attribute of an unstructured field that match the selector
  * 'abc.S("div#a1").attrs("src"): first "src" attribute of an unstructured field that match the selector
  */
sealed trait Level2 {

  import org.apache.spark.ml.dsl.utils.refl.ScalaType._
  import org.apache.spark.sql.catalyst.ScalaReflection.universe.TypeTag

  implicit class ExView[R: ClassTag](self: Extractor[R])(
      implicit val defaultV: Default[R]
  ) extends Serializable {

    def into(field: Field) = Append.create[R](field, self)
    def ~+(field: Field) = into(field)
  }

  implicit class StringExView(self: Extractor[String]) extends Serializable {

    def replaceAll(regex: String, replacement: String): Extractor[String] =
      self.andFn(_.replaceAll(regex, replacement))

    def trim: Extractor[String] = self.andFn(_.trim)

    def +(another: Extractor[Any]): Extractor[String] = x"$self$another"
  }

  implicit class UnstructuredExView(self: Extractor[Unstructured]) extends Serializable {

    def uri: Extractor[String] = self.andFn(_.uri)

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

    def expand(range: Range) = {
      self match {
        case AndThen(_, _, Some(FindAllMeta(argg, selector))) =>
          argg.andFn(_.findAllWithSiblings(selector, range))
        case AndThen(_, _, Some(ChildrenMeta(argg, selector))) =>
          argg.andFn(_.childrenWithSiblings(selector, range))
        case _ =>
          throw new UnsupportedOperationException("expression does not support expand")
      }
    }
  }

  implicit class ElementsExView(self: Extractor[Elements[_]]) extends Serializable {

    def uris: Extractor[Seq[String]] = self.andFn(_.uris)

    def texts: Extractor[Seq[String]] = self.andFn(_.texts)

    def codes: Extractor[Seq[String]] = self.andFn(_.codes)

    def ownTexts: Extractor[Seq[String]] = self.andFn(_.ownTexts)

    def allAttrs: Extractor[Seq[Map[String, String]]] =
      self.andFn(_.allAttrs)

    def attrs(attrKey: String, noEmpty: Boolean = true): Extractor[Seq[String]] =
      self.andFn(_.attrs(attrKey, noEmpty))

    def hrefs = self.andFn(_.hrefs)

    def srcs = self.andFn(_.srcs)

    def boilerPipes = self.andFn(_.boilerPipes)
  }

  implicit class DocExView(self: Extractor[Doc]) extends UnstructuredExView(self.andFn(_.root)) {

    def uid: Extractor[DocUID] = self.andFn(_.uid)

    def contentType: Extractor[String] = self.andFn(_.contentType)

    def content: Extractor[Seq[Byte]] = self.andFn(_.raw.toSeq)

    def timestamp: Extractor[Timestamp] = self.andFn(_.timestamp)

    def saved: Extractor[Set[String]] = self.andFn(_.saved.toSet)

    def mimeType: Extractor[String] = self.andFn(_.mimeType)

    def charSet: Extractor[String] = self.andOptionFn(_.charset)

    def fileExtensions: Extractor[Seq[String]] = self.andFn(_.fileExtensions.toSeq)

    def defaultFileExtension: Extractor[String] = self.andOptionFn(_.defaultFileExtension)
  }

  implicit def SeqMagnetExView[T: TypeTag](self: Extractor[HasSeq[T]]): IterableExView[T] = {
    new IterableExView[T](self.andFn(v => v.seq))
  }

  implicit class IterableExView[T: TypeTag](self: Extractor[Iterable[T]]) extends Serializable {

    def head: Extractor[T] = self.andOptionTyped((v: Iterable[T]) => v.headOption, _.unboxArrayOrMap)

    def last: Extractor[T] = self.andOptionTyped((v: Iterable[T]) => v.lastOption, _.unboxArrayOrMap)

    def get(i: Int): Extractor[T] =
      self.andOptionTyped({ (v: Iterable[T]) =>
        val realIdx =
          if (i >= 0) i
          else v.size - i

        if (realIdx >= v.size || realIdx < 0) None
        else Some(v.toSeq.apply(realIdx))
      }, _.unboxArrayOrMap)

    def size: Extractor[Int] = self.andFn(_.size)

    def isEmpty: Extractor[Boolean] = self.andFn(_.isEmpty)

    def nonEmpty: Extractor[Boolean] = self.andFn(_.nonEmpty)

    def mkString(sep: String = ""): Extractor[String] = self.andFn(_.mkString(sep))

    def mkString(start: String, sep: String, end: String): Extractor[String] = self.andFn(_.mkString(start, sep, end))

    //TODO: Why IterableExView.filter cannot be applied on ZippedExpr? is the scala compiler malfunctioning?
    def zipWithKeys(keys: Extractor[Any]): Zipped[Any, T] =
      new Zipped[Any, T](keys.typed[Iterable[_]], self)

    def zipWithValues(values: Extractor[Any]): Zipped[T, Any] =
      new Zipped[T, Any](self, values.typed[Iterable[_]])

    protected def groupByImpl[K](f: T => K): Iterable[T] => Map[K, Seq[T]] =
      (v: Iterable[T]) => v.groupBy(f).mapValues(_.toSeq)

    def groupBy[K: TypeTag](f: T => K): Extractor[Map[K, Seq[T]]] = {

      val keyType = UnreifiedObjectType.summon[K]

      self.andTyped(
        groupByImpl(f), { t =>
          MapType(keyType, t)
        }
      )
    }

    def slice(from: Int = Int.MinValue, until: Int = Int.MaxValue): Extractor[Iterable[T]] = self.andOptionTyped(
      (v: Iterable[T]) => Some(v.slice(from, until)),
      identity
    )

    def filter(f: T => Boolean): Extractor[Iterable[T]] =
      self.andOptionTyped((v: Iterable[T]) => Some(v.filter(f)), identity)

    def distinct: Extractor[Seq[T]] = self.andOptionTyped((v: Iterable[T]) => Some(v.toSeq.distinct), identity)

    def distinctBy[K](f: T => K): Extractor[Iterable[T]] = {
      self.andTyped(
        groupByImpl(f)
          .andThen(
            v =>
              v.values.flatMap {
                case repr: Traversable[T] => repr.headOption
                case _                    => None //TODO: what's the point of this? removed
            }
          ),
        identity
      )
    }

    def map[B: TypeTag](f: T => B): Extractor[Seq[B]] = self.andFn(
      v => v.toSeq.map(f)
    )

    def flatMap[B: TypeTag](f: T => GenTraversableOnce[B]): Extractor[Seq[B]] = self.andFn(
      v => v.toSeq.flatMap(f)
    )
  }

  //--------------------------------------------------

  implicit def symbol2Field(symbol: Symbol): Field =
    Option(symbol).map(v => Field(v.name)).orNull

  implicit def symbol2Get(symbol: Symbol): Get =
    Get(symbol.name)

  implicit def symbolToDocExView(symbol: Symbol): DocExView =
    GetDocExpr(symbol.name)

  implicit def symbol2GetItr(symbol: Symbol): IterableExView[Any] =
    IterableExView(Get(symbol.name).GetSeq)
}

sealed trait Level1 extends Level2 {

  implicit def symbol2GetUnstructured(symbol: Symbol): UnstructuredExView =
    GetUnstructuredExpr(symbol.name)

  implicit class StrContextHelper(val strC: StringContext) extends Serializable {

    def x(parts: Col[String]*) = Interpolate(strC.parts, parts.map(_.ex))

    def CSS(parts: Col[String]*) = GetOnlyDocExpr.andFn(_.root).findAll(strC.s(parts: _*))
    def S(parts: Col[String]*) = CSS(parts: _*)

    def CSS_*(parts: Col[String]*) = GetAllDocsExpr.findAll(strC.s(parts: _*))
    def S_*(parts: Col[String]*) = CSS_*(parts: _*)

    def A(parts: Col[String]*) = 'A.findAll(strC.s(parts: _*))
  }

  implicit def FDToRDD(self: FetchedDataset): RDD[FetchedRow] = self.rdd

  implicit def spookyContextToFD(spooky: SpookyContext): FetchedDataset = spooky.createBlank

  implicit def traceView(trace: Trace): TraceView = new TraceView(trace)

  implicit def traceSetView[Repr](traces: Repr)(implicit f: Repr => Set[Trace]): TraceSetView = new TraceSetView(traces)

  implicit def actionToTraceSet(action: Action): Set[Trace] = Set(List(action))
}

class DSL extends Level1 {

  import com.tribbloids.spookystuff.extractors.impl.Extractors._

  def S: GenExtractor[FR, Doc] = GetOnlyDocExpr
  def S(selector: String): GenExtractor[FR, Elements[Unstructured]] = S.andFn(_.root).findAll(selector)
  //  def S(selector: String, i: Int): Extractor[Unstructured] = {
  //    val expr = this.S(selector)
  //    new IterableExView(expr).get(i)
  //  }
  def `S_*`: GenExtractor[FR, Elements[Unstructured]] = GetAllDocsExpr
  def S_*(selector: String): GenExtractor[FR, Elements[Unstructured]] = `S_*`.findAll(selector)
  //  def S_*(selector: String, i: Int): Extractor[Unstructured] = {
  //    val expr = GetAllPagesExpr.findAll(selector)
  //    new IterableExView(expr).get(i)
  //  }

  def G = GroupIndexExpr

  def A(selector: String) = 'A.findAll(selector)
  def A(selector: String, i: Int): Extractor[Unstructured] = {
    val expr = 'A.findAll(selector)
    expr.get(i)
  }
}

object DSL extends DSL
