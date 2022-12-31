package com.tribbloids.spookystuff.extractors

import com.tribbloids.spookystuff.doc._
import com.tribbloids.spookystuff.extractors.GenExtractor.AndThen
import com.tribbloids.spookystuff.extractors.impl.Extractors._
import com.tribbloids.spookystuff.extractors.impl.{Append, Zipped}
import com.tribbloids.spookystuff.row.Field
import com.tribbloids.spookystuff.utils.Default
import org.apache.spark.ml.dsl.utils.refl.UnreifiedObjectType
import org.apache.spark.sql.types.MapType

import java.sql.Timestamp
import scala.collection.GenTraversableOnce
import scala.language.implicitConversions
import scala.reflect.ClassTag

trait GenExtractorImplicits {

  import com.tribbloids.spookystuff.dsl.DSL._
  import org.apache.spark.ml.dsl.utils.refl.TypeMagnet._
  import org.apache.spark.sql.catalyst.ScalaReflection.universe
  import universe.TypeTag

  implicit class ExView[R: ClassTag](self: Extractor[R])(
      implicit
      val defaultV: Default[R]
  ) extends Serializable {

    def into(field: Field): Alias[FR, Seq[R]] = Append.create[R](field, self)
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

    def findAll(selector: String): GenExtractor[FR, Elements[Unstructured]] = FindAllExpr(self, selector)
    def \\(selector: String) = findAll(selector)
    def findFirst(selector: String): Extractor[Unstructured] = findAll(selector).head

    def children(selector: String): GenExtractor[FR, Elements[Unstructured]] = ChildrenExpr(self, selector)
    def \(selector: String) = children(selector)
    def child(selector: String): Extractor[Unstructured] = children(selector).head

    def text: Extractor[String] = self.andOptionFn { v =>
      v.text
    }

    def code: GenExtractor[FR, String] = self.andOptionFn(_.code)

    def formattedCode: GenExtractor[FR, String] = self.andOptionFn(_.formattedCode)

    def ownText: Extractor[String] = self.andOptionFn(_.ownText)

    def allAttr: Extractor[Map[String, String]] =
      self.andOptionFn(_.allAttr)

    def attr(attrKey: String, noEmpty: Boolean = true): Extractor[String] =
      self.andOptionFn(_.attr(attrKey, noEmpty))

    def href: GenExtractor[FR, String] = self.andOptionFn(_.href)

    def src: GenExtractor[FR, String] = self.andOptionFn(_.src)

    def boilerPipe: GenExtractor[FR, String] = self.andOptionFn(_.boilerPipe)

    def expand(range: Range): GenExtractor[FR, Elements[Siblings[Unstructured]]] = {
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

    def hrefs: GenExtractor[FR, List[String]] = self.andFn(_.hrefs)

    def srcs: GenExtractor[FR, List[String]] = self.andFn(_.srcs)

    def boilerPipes: GenExtractor[FR, Seq[String]] = self.andFn(_.boilerPipes)
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

  implicit def SeqMagnetExView[T: TypeTag](self: Extractor[DelegateSeq[T]]): IterableExView[T] = {
    new IterableExView[T](self.andFn(v => v.seq))
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

    def size: Extractor[Int] = self.andFn(_.size)

    def isEmpty: Extractor[Boolean] = self.andFn(_.isEmpty)

    def nonEmpty: Extractor[Boolean] = self.andFn(_.nonEmpty)

    def mkString(sep: String = ""): Extractor[String] = self.andFn(_.mkString(sep))

    def mkString(start: String, sep: String, end: String): Extractor[String] = self.andFn(_.mkString(start, sep, end))

    // TODO: Why IterableExView.filter cannot be applied on ZippedExpr? is the scala compiler malfunctioning?
    def zipWithKeys(keys: Extractor[Any]): Zipped[Any, T] =
      new Zipped[Any, T](keys.typed[Iterable[_]], self)

    def zipWithValues(values: Extractor[Any]): Zipped[T, Any] =
      new Zipped[T, Any](self, values.typed[Iterable[_]])

    protected def groupByImpl[K](f: T => K): Iterable[T] => Map[K, Seq[T]] =
      (v: Iterable[T]) => v.groupBy(f).view.mapValues(_.toSeq).toMap

    def groupBy[K: TypeTag](f: T => K): Extractor[Map[K, Seq[T]]] = {

      val keyType = UnreifiedObjectType.summon[K]

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
              case repr: Traversable[T] => repr.headOption
              case _                    => None // TODO: what's the point of this? removed
            }
          ),
        identity
      )
    }

    def map[B: TypeTag](f: T => B): Extractor[Seq[B]] = self.andFn(v => v.toSeq.map(f))

    def flatMap[B: TypeTag](f: T => GenTraversableOnce[B]): Extractor[Seq[B]] = self.andFn(v => v.toSeq.flatMap(f))
  }
}

object GenExtractorImplicits {}
