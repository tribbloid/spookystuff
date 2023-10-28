package com.tribbloids.spookystuff.extractors

import com.tribbloids.spookystuff.doc._
import com.tribbloids.spookystuff.extractors.GenExtractor.Chain
import com.tribbloids.spookystuff.extractors.impl.Extractors._
import com.tribbloids.spookystuff.extractors.impl.Fold.AppendSeq
import com.tribbloids.spookystuff.extractors.impl.{Fold, Get, Zipped}
import com.tribbloids.spookystuff.row.Alias
import org.apache.spark.ml.dsl.utils.refl.{CatalystTypeOps, TypeMagnet}
import org.apache.spark.sql.types.MapType

import java.sql.Timestamp
import scala.language.implicitConversions
import scala.reflect.ClassTag

trait GenExtractorImplicits extends CatalystTypeOps.ImplicitMixin {

  import com.tribbloids.spookystuff.dsl.DSL._
  import org.apache.spark.sql.catalyst.ScalaReflection.universe
  import universe.TypeTag

  implicit class ExtractorView[R: ClassTag](val self: Extractor[R]) extends Serializable {

    def as(alias: Alias): GenExtractor[FR, R] =
      Fold.FailFast(Get(alias), self).withAliasOpt(Option(alias))
    def ~(alias: Alias): GenExtractor[FR, R] = as(alias)

    def replace(alias: Alias): GenExtractor[FR, R] =
      Fold.PreferNew[R](Get(alias).cast[R], self).withAliasOpt(Option(alias))
    def ~!(alias: Alias): GenExtractor[FR, R] = replace(alias)

    def dropAndReplace(alias: Alias): GenExtractor[FR, R] = self.withAliasOpt(Option(alias))
    def ~!!(alias: Alias): GenExtractor[FR, R] = dropAndReplace(alias)

    def append(alias: Alias): HasAlias[FR, Seq[R]] = AppendSeq[R](Get(alias), self).withAlias(alias)
    def ~+(alias: Alias): HasAlias[FR, Seq[R]] = append(alias)
  }

  object ExtractorView {

    implicit def unbox[R](v: ExtractorView[R]): Extractor[R] = v.self
  }

  implicit class StringExView(self: Extractor[String]) extends Serializable {

    def replaceAll(regex: String, replacement: String): Extractor[String] =
      self.andMap(_.replaceAll(regex, replacement))

    def trim: Extractor[String] = self.andMap(_.trim)

    def +(another: Extractor[Any]): Extractor[String] = x"$self$another"
  }

  implicit class UnstructuredExView(self: Extractor[Unstructured]) extends Serializable {

    def uri: Extractor[String] = self.andMap(_.uri)

    def findAll(selector: String): GenExtractor[FR, Elements[Unstructured]] = FindAllExpr(self, selector)
    def \\(selector: String): GenExtractor[FR, Elements[Unstructured]] = findAll(selector)
    def findFirst(selector: String): Extractor[Unstructured] = findAll(selector).head

    def children(selector: String): GenExtractor[FR, Elements[Unstructured]] = ChildrenExpr(self, selector)
    def \(selector: String): GenExtractor[FR, Elements[Unstructured]] = children(selector)
    def child(selector: String): Extractor[Unstructured] = children(selector).head

    def text: Extractor[String] = self.andFlatMap { v =>
      v.text
    }

    def code: GenExtractor[FR, String] = self.andFlatMap(_.code)

    def formattedCode: GenExtractor[FR, String] = self.andFlatMap(_.formattedCode)

    def ownText: Extractor[String] = self.andFlatMap(_.ownText)

    def allAttr: Extractor[Map[String, String]] =
      self.andFlatMap(_.allAttr)

    def attr(attrKey: String, noEmpty: Boolean = true): Extractor[String] =
      self.andFlatMap(_.attr(attrKey, noEmpty))

    def href: GenExtractor[FR, String] = self.andFlatMap(_.href)

    def src: GenExtractor[FR, String] = self.andFlatMap(_.src)

    def boilerPipe: GenExtractor[FR, String] = self.andFlatMap(_.boilerPipe)

    def expand(range: Range): GenExtractor[FR, Elements[Siblings[Unstructured]]] = {
      self match {
        case Chain(_, _, Some(FindAllMeta(argg, selector))) =>
          argg.andMap(_.findAllWithSiblings(selector, range))
        case Chain(_, _, Some(ChildrenMeta(argg, selector))) =>
          argg.andMap(_.childrenWithSiblings(selector, range))
        case _ =>
          throw new UnsupportedOperationException("expression does not support expand")
      }
    }
  }

  implicit class ElementsExView(self: Extractor[Elements[_]]) extends Serializable {

    def uris: Extractor[Seq[String]] = self.andMap(_.uris)

    def texts: Extractor[Seq[String]] = self.andMap(_.texts)

    def codes: Extractor[Seq[String]] = self.andMap(_.codes)

    def ownTexts: Extractor[Seq[String]] = self.andMap(_.ownTexts)

    def allAttrs: Extractor[Seq[Map[String, String]]] =
      self.andMap(_.allAttrs)

    def attrs(attrKey: String, noEmpty: Boolean = true): Extractor[Seq[String]] =
      self.andMap(_.attrs(attrKey, noEmpty))

    def hrefs: GenExtractor[FR, List[String]] = self.andMap(_.hrefs)

    def srcs: GenExtractor[FR, List[String]] = self.andMap(_.srcs)

    def boilerPipes: GenExtractor[FR, Seq[String]] = self.andMap(_.boilerPipes)
  }

  implicit class DocExView(self: Extractor[Doc]) extends UnstructuredExView(self.andMap(_.root)) {

    def uid: Extractor[DocUID] = self.andMap(_.uid)

    def contentType: Extractor[String] = self.andMap(_.contentType)

    def content: Extractor[Seq[Byte]] = self.andMap(_.raw.toSeq)

    def timestamp: Extractor[Timestamp] = self.andMap(_.timestamp)

    def saved: Extractor[Set[String]] = self.andMap(_.saved.toSet)

    def mimeType: Extractor[String] = self.andMap(_.mimeType)

    def charSet: Extractor[String] = self.andFlatMap(_.charset)

    def fileExtensions: Extractor[Seq[String]] = self.andMap(_.fileExtensions.toSeq)

    def defaultFileExtension: Extractor[String] = self.andFlatMap(_.defaultFileExtension)
  }

  implicit def SeqMagnetExView[T: TypeTag](self: Extractor[DelegateSeq[T]]): IterableExView[T] = {
    new IterableExView[T](self.andMap(v => v.seq))
  }

  implicit class IterableExView[T: TypeTag](self: Extractor[Iterable[T]]) extends Serializable {

    def head: Extractor[T] = self.andOptionTyped((v: Iterable[T]) => v.headOption, _.unboxArrayOrMap)

    def last: Extractor[T] = self.andOptionTyped((v: Iterable[T]) => v.lastOption, _.unboxArrayOrMap)

    def get(i: Int): Extractor[T] =
      self.andOptionTyped(
        { (v: Iterable[T]) =>
          val realIdx =
            if (i >= 0) i
            else v.size - i

          if (realIdx >= v.size || realIdx < 0) None
          else Some(v.toSeq.apply(realIdx))
        },
        _.unboxArrayOrMap
      )

    def size: Extractor[Int] = self.andMap(_.size)

    def isEmpty: Extractor[Boolean] = self.andMap(_.isEmpty)

    def nonEmpty: Extractor[Boolean] = self.andMap(_.nonEmpty)

    def mkString(sep: String = ""): Extractor[String] = self.andMap(_.mkString(sep))

    def mkString(start: String, sep: String, end: String): Extractor[String] = self.andMap(_.mkString(start, sep, end))

    // TODO: Why IterableExView.filter cannot be applied on ZippedExpr? is the scala compiler malfunctioning?
    def zipWithKeys(keys: Extractor[Any]): Zipped[Any, T] =
      new Zipped[Any, T](keys.filterByType[Iterable[_]], self)

    def zipWithValues(values: Extractor[Any]): Zipped[T, Any] =
      new Zipped[T, Any](self, values.filterByType[Iterable[_]])

    protected def groupByImpl[K](f: T => K): Iterable[T] => Map[K, Seq[T]] =
      (v: Iterable[T]) => v.groupBy(f).view.mapValues(_.toSeq).toMap

    def groupBy[K: TypeTag](f: T => K): Extractor[Map[K, Seq[T]]] = {

      val keyType = TypeMagnet.FromTypeTag[K].asCatalystTypeOrUnknown

      self.andTyped(
        groupByImpl(f),
        { t =>
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
          .andThen(v =>
            v.values.flatMap {
              case repr: Iterable[T] => repr.headOption
              case _                 => None // TODO: what's the point of this? removed
            }
          ),
        identity
      )
    }

    def map[B: TypeTag](f: T => B): Extractor[Seq[B]] = self.andMap(v => v.toSeq.map(f))

//    def flatMap[B: TypeTag](f: T => IterableOnce[B]): Extractor[Seq[B]] = self.andMap(v => v.toSeq.flatMap(f))
  }
}

object GenExtractorImplicits {}
