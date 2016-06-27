package com.tribbloids.spookystuff.extractors

import com.tribbloids.spookystuff.Const
import com.tribbloids.spookystuff.extractors.GenExtractor._
import com.tribbloids.spookystuff.row.Field
import com.tribbloids.spookystuff.utils.{PrettyToStringMixin, Utils}
import org.apache.spark.sql.TypeUtils
import org.apache.spark.sql.catalyst.trees.TreeNode
import org.apache.spark.sql.types.{DataType, StructField, StructType}

import scala.language.implicitConversions

object GenExtractor {

  final val functionVID = -592849327L

  def fromFn[T, R](self: (T) => R, dataType: DataType): GenExtractor[T, R] = {
    Elem(_ => Partial(self), _ => dataType)
  }
  implicit def fromFn[T, R: TypeTag](self: T => R): GenExtractor[T, R] = {

    val dataType = TypeUtils.catalystTypeOrDefault[R]()

    fromFn(self, dataType)
  }

  def fromOptionFn[T, R](self: (T) => Option[R], dataType: DataType): GenExtractor[T, R] = {
    Elem(_ => Unlift(self), _ => dataType)
  }
  def fromOptionFn[T, R: TypeTag](self: T => Option[R]): GenExtractor[T, R] = {

    //this works under the assumption that Catalyst always converts Option[A] to the same DataType as A
    val dataType = TypeUtils.catalystTypeOrDefault[R]()

    fromOptionFn(self, dataType)
  }

  trait Leaf[T, +R] extends GenExtractor[T, R] {
    override def _args = Nil
  }
  trait Unary[T, +R] extends GenExtractor[T, R] {
    override def _args = Seq(child)
    def child: GenExtractor[_,_]
  }

  trait StaticType[T, +R] extends GenExtractor[T,R] {
    val dataType: DataType
    final def resolveType(tt: DataType) = dataType
  }
  trait StaticPartialFunction[T, +R] extends GenExtractor[T,R] with PartialFunctionWrapper[T, R] with PrettyToStringMixin{
    final def resolve(tt: DataType) = self
  }
  trait Static[T, +R] extends StaticType[T,R] with StaticPartialFunction[T, R] with Leaf[T, R]

  trait Wrapper[T, +R] extends Unary[T, R] {

    def child: GenExtractor[T, R]

    def resolveType(dataType: DataType) = child.resolveType(dataType)
    def resolve(dataType: DataType) = child.resolve(dataType)
  }

  case class Elem[T, +R](
                          _resolve: DataType => PartialFunction[T, R],
                          _resolveType: DataType => DataType,
                          name: Option[String] = None
                        ) extends Leaf[T, R]{
    //resolve to a Spark SQL DataType according to an exeuction plan
    override def resolveType(tt: DataType): DataType = _resolveType(tt)

    override def resolve(tt: DataType): PartialFunction[T, R] = _resolve(tt)

    //    override def toString = meta.getOrElse("Elem").toString
  }

  case class AndThen[A, B, +C](
                                a: GenExtractor[A, B],
                                b: GenExtractor[B, C],
                                meta: Option[Any] = None
                              ) extends GenExtractor[A, C] {

    //resolve to a Spark SQL DataType according to an exeuction plan
    override def resolveType(tt: DataType): DataType = b.resolveType(a.resolveType(tt))

    override def resolve(tt: DataType): PartialFunction[A, C] = {
      val af = a.resolve(tt)
      val bf = b.resolve(tt)
      Unlift(af.lift.andThen(_.flatMap(v => bf.lift(v))))
    }

    //TODO: changing to Unary? Like Spark SQL Expression
    override def _args: Seq[GenExtractor[_, _]] = Seq(a, b)
  }

  case class And_->[T, +R1, +R2](
                                  arg1: GenExtractor[T, R1],
                                  arg2: GenExtractor[T, R2]
                                ) extends GenExtractor[T, (R1, R2)] {
    //resolve to a Spark SQL DataType according to an exeuction plan
    override def resolveType(tt: DataType): DataType = {
      val t1 = arg1.resolveType(tt)
      val t2 = arg2.resolveType(tt)

      StructType(Seq(
        StructField("_1", t1),
        StructField("_2", t2)
      ))
    }

    override def resolve(tt: DataType): PartialFunction[T, (R1, R2)] = {
      val r1 = arg1.resolve(tt).lift
      val r2 = arg2.resolve(tt).lift
      Unlift({
        t =>
          r1.apply(t).flatMap(
            v =>
              r2.apply(t).map(
                vv =>
                  v -> vv
              )
          )
      })
    }

    override def _args: Seq[GenExtractor[_, _]] = Seq(arg1, arg2)
  }

  case class TreeNodeView(self: GenExtractor[_,_]) extends TreeNode[TreeNodeView] {
    override def children: Seq[TreeNodeView] = self._args.map(TreeNodeView)
  }
}

// a special expression that can be applied on:
// 1. FetchedRow, yielding a datum with dataType to be used in .extract() and .explore()
// 2. (To be implemented) Internal Row backing a SquashedFetchedRow, this makes extractor an expression itself and can be wrapped by COTS expressions.

// a subclass wraps an expression and convert it into extractor, which converts all attribute reference children into data reference children and
// (To be implemented) can be converted to an expression to be wrapped by other expressions
trait GenExtractor[T, +R] extends Product with Serializable {

  lazy val TreeNode: GenExtractor.TreeNodeView = GenExtractor.TreeNodeView(this)

  protected def _args: Seq[GenExtractor[_, _]]

  //resolve to a Spark SQL DataType according to an exeuction plan
  def resolveType(tt: DataType): DataType
  def resolve(tt: DataType): PartialFunction[T, R]

  def withAlias(field: Field): AliasImpl[T, R] = {
    this match {
      case v: Wrapper[T, R] => new AliasImpl[T, R](v.child, field)
      case _ => new AliasImpl[T, R](this, field)
    }
  }
  def withoutAlias: GenExtractor[T, R] = {
    this match {
      case v: Wrapper[T, R] => v.child
      case _ => this
    }
  }

  private def _as(fieldOpt: Option[Field]): GenExtractor[T, R] = {

    fieldOpt match {
      case Some(field) => withAlias(field)
      case None => withoutAlias
    }
  }

  final def as(field: Field) = _as(Option(field))
  final def ~(field: Field) = as(field)
  //  final def as_!(field: Field) = _as(Option(field).map(_.!))
  //  final def ~!(field: Field) = as_!(field)
  //  final def as_*(field: Field) = _as(Option(field).map(_.*))
  //  final def ~*(field: Field) = as_*(field)

  //  final def named_!(field: Field) = _named(field.!)
  //  final def named_*(field: Field) = _named(field.*)

  //will not rename an already-named Alias.
  def withAliasIfMissing(field: Field): Alias[T, R] = {
    this match {
      case alias: Alias[T, R] => alias
      case _ => this.withAlias(field)
    }
  }

  def withJoinFieldIfMissing = withAliasIfMissing(Const.defaultJoinField)

  def andEx[R2>: R, A](g: GenExtractor[R2, A], meta: Option[Any] = None): GenExtractor[T, A] = AndThen[T, R2, A](this, g, meta)

  def andThen[A: TypeTag](g: R => A, meta: Option[Any] = None): GenExtractor[T, A] = {
    andEx(g, meta)
  }

  def andOptionFn[A: TypeTag](g: R => Option[A], meta: Option[Any] = None): GenExtractor[T, A] = {
    andEx(GenExtractor.fromOptionFn(g), meta)
  }

  def andTyped[R2 >: R, A](
                            g: R2 => A,
                            resolveType: DataType => DataType,
                            meta: Option[Any] = None
                          ) = andEx (
    Elem[R2, A](_ => Partial(g), resolveType),
    meta
  )

  def andOptionTyped[R2 >: R, A](
                                  g: R2 => Option[A],
                                  resolveType: DataType => DataType,
                                  meta: Option[Any] = None
                                ) = andTyped(Unlift(g), resolveType, meta)

  import TypeUtils.Implicits._

  //TODO: extract subroutine and use it to avoid obj creation overhead
  def typed[A: TypeTag]: GenExtractor[T, A] = {
    implicit val ctg = implicitly[TypeTag[A]].toClassTag

    andOptionFn[A]{
      Utils.typedOrNone[A]
    }
  }

  def toStr = andThen(_.toString)
}

trait Alias[T, +R] extends GenExtractor[T, R] {

  def field: Field
}

case class AliasImpl[T, +R](
                             child: GenExtractor[T, R],
                             field: Field
                           ) extends Alias[T, R] with Wrapper[T, R] {
}

