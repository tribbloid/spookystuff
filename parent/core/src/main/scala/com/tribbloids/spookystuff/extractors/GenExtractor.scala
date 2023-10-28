package com.tribbloids.spookystuff.extractors

import com.tribbloids.spookystuff.Const
import com.tribbloids.spookystuff.row.Alias
import com.tribbloids.spookystuff.tree.TreeView
import com.tribbloids.spookystuff.utils.SpookyUtils
import com.tribbloids.spookystuff.relay.AutomaticRelay
import org.apache.spark.ml.dsl.utils.refl.{CatalystTypeOps, ReflectionLock, TypeMagnet}
import org.apache.spark.sql.catalyst.ScalaReflection.universe
import org.apache.spark.sql.catalyst.ScalaReflection.universe.TypeTag

import scala.language.{existentials, implicitConversions}
import scala.reflect.ClassTag

object GenExtractor extends AutomaticRelay[GenExtractor[_, _]] with GenExtractorImplicits {

//  final val functionVID = -592849327L

  abstract class Elem[T, +R](
      _resolve: DataType => (T => R),
      _resolveType: DataType => DataType
  ) extends Leaf[T, R] {
    // resolve to a Spark SQL DataType according to an exeuction plan
    override def resolveType(tt: DataType): DataType = _resolveType(tt)

    override def resolve(tt: DataType): PartialFunction[T, R] = {

      val result = _resolve(tt)
      result match {
        case v: PartialFunction[_, _] => v
        case _ =>
          PartialFunction.fromFunction(result)
      }
    }

    //    override def toString = meta.getOrElse("Elem").toString
  }

  case class fromFn[T, R](dataType: DataType)(self: T => R) extends Elem(_ => self, _ => dataType)

  case class fromPoly[T, R]()(poly: DataType => DataType, self: T => R) extends Elem(_ => self, poly)

  case class fromOptionFn[T, R](dataType: DataType)(self: T => Option[R]) extends Elem(_ => Unlift(self), _ => dataType)

  def fromOptionFn[T, R: TypeTag](self: T => Option[R]): GenExtractor[T, R] = {

    fromOptionFn(TypeMagnet.summon[R].asCatalystTypeOrUnknown)(self)
  }

  trait Leaf[T, +R] extends GenExtractor[T, R] {
    override def _args: Seq[GenExtractor[_, _]] = Nil
  }
  trait Unary[T, +R] extends GenExtractor[T, R] {
    override def _args: Seq[GenExtractor[_, _]] = Seq(child)
    def child: GenExtractor[_, _]
  }

  // TODO: possibility to merge into Spark Resolved expression?
  trait StaticType[T, +R] extends GenExtractor[T, R] {
    val dataType: DataType
    final def resolveType(tt: DataType): DataType = dataType
  }
  trait Static[T, +R] extends StaticType[T, R] with GenExtractor[T, R] with Leaf[T, R] {

    def resolved: PartialFunction[T, R]

    final def resolve(tt: DataType): PartialFunction[T, R] = resolved
  }

  object Static {

    implicit def unbox[T, R](v: Static[T, R]): PartialFunction[T, R] = v.resolved
  }

  trait Wrapper[T, +R] extends Unary[T, R] {

    def child: GenExtractor[T, R]

    def resolveType(dataType: DataType): DataType = child.resolveType(dataType)
    def resolve(dataType: DataType): PartialFunction[T, R] = child.resolve(dataType)
  }

  case class Chain[A, B, +C](
      a: GenExtractor[A, B],
      b: GenExtractor[B, C],
      meta: Option[Any] = None
  ) extends GenExtractor[A, C] {

    // resolve to a Spark SQL DataType according to an exeuction plan
    override def resolveType(tt: DataType): DataType = b.resolveType(a.resolveType(tt))

    override def resolve(tt: DataType): PartialFunction[A, C] = {
      val af = a.resolve(tt)
      val bf = b.resolve(tt)
      Unlift(af.lift.andThen(_.flatMap(v => bf.lift(v))))
    }

    // TODO: changing to Unary? Like Spark SQL Expression
    override def _args: Seq[GenExtractor[_, _]] = Seq(a, b)
  }

  case class ExtractTuple[T, +R1, +R2](
      arg1: GenExtractor[T, R1],
      arg2: GenExtractor[T, R2]
  ) extends GenExtractor[T, (R1, R2)] {
    // resolve to a Spark SQL DataType according to an execution plan
    override def resolveType(tt: DataType): DataType = locked {
      val t1 = arg1.resolveType(tt)
      val t2 = arg2.resolveType(tt)

      val typeMagnet = (
        t1.typeTag_wild,
        t2.typeTag_wild
      ) match {
        case (ttg1: TypeTag[a], ttg2: TypeTag[b]) =>
          implicit val t1: universe.TypeTag[a] = ttg1
          implicit val t2: universe.TypeTag[b] = ttg2
          implicitly[TypeMagnet[(a, b)]]
      }

      typeMagnet.asCatalystTypeOrUnknown
    }

    override def resolve(tt: DataType): PartialFunction[T, (R1, R2)] = {
      val r1 = arg1.resolve(tt).lift
      val r2 = arg2.resolve(tt).lift
      Unlift { t =>
        r1.apply(t)
          .flatMap(v =>
            r2.apply(t)
              .map(vv => v -> vv)
          )
      }
    }

    override def _args: Seq[GenExtractor[_, _]] = Seq(arg1, arg2)
  }

  case class TreeNodeView(self: GenExtractor[_, _]) extends TreeView.Immutable[TreeNodeView] {
    override def children: Seq[TreeNodeView] = self._args.map(TreeNodeView)

  }

  // ------------implicits-------------

  implicit def fromFn[T, R: TypeTag](self: T => R): GenExtractor[T, R] = {

    fromFn(TypeMagnet.summon[R].asCatalystTypeOrUnknown)(self)
  }

  trait HasAlias[T, +R] extends GenExtractor[T, R] {

    def alias: Alias
  }

  case class Aliased[T, +R](
      child: GenExtractor[T, R],
      alias: Alias
  ) extends HasAlias[T, R]
      with GenExtractor.Wrapper[T, R] {}
}

// a special expression that can be applied on:
// 1. FetchedRow, yielding a datum with dataType to be used in .extract() and .explore()
// 2. (To be implemented) Internal Row backing a SquashedFetchedRow, this makes extractor an expression itself and can be wrapped by COTS expressions.

// a subclass wraps an expression and convert it into extractor, which converts all attribute reference children into data reference children and
// (To be implemented) can be converted to an expression to be wrapped by other expressions
trait GenExtractor[T, +R] extends ReflectionLock with CatalystTypeOps.ImplicitMixin with Product with Serializable {

  import com.tribbloids.spookystuff.extractors.GenExtractor._

  lazy val TreeNode: GenExtractor.TreeNodeView = GenExtractor.TreeNodeView(this)

  protected def _args: Seq[GenExtractor[_, _]]

  // TODO: this is mapped from a Scala/shapeless polymorphic function, which is too complex and
  //   totally unnecessary if compile-time type inference (e.g. frameless) is used ahead-of-time
  // TODO: can use TypeMagnet to reduce boilerplate
  // resolve to a Spark SQL DataType according to an execution plan
  // TODO: the following 2 can be merged into a single method, returning a lazy tuple
  def resolveType(tt: DataType): DataType
  def resolve(tt: DataType): PartialFunction[T, R]

  def withAlias(alias: Alias): Aliased[T, R] = {
    this match {
      case v: Wrapper[T, R] => new Aliased[T, R](v.child, alias)
      case _                => new Aliased[T, R](this, alias)
    }
  }
  def withoutAlias: GenExtractor[T, R] = {
    this match {
      case v: Wrapper[T, R] => v.child
      case _                => this
    }
  }

  def withAliasOpt(fieldOpt: Option[Alias]): GenExtractor[T, R] = {

    fieldOpt match {
      case Some(field) => withAlias(field)
      case None        => withoutAlias
    }
  }

  // will not rename an already-named Alias.
  def withAliasIfMissing(alias: Alias): HasAlias[T, R] = {
    this match {
      case alias: HasAlias[T, R] => alias
      case _                     => this.withAlias(alias)
    }
  }

  def withJoinFieldIfMissing: HasAlias[T, R] = withAliasIfMissing(Const.defaultJoin)

  // TODO: should merge into andMap
  def andEx[R2 >: R, A](g: GenExtractor[R2, A], meta: Option[Any] = None): GenExtractor[T, A] =
    Chain[T, R2, A](this, g, meta)

  def andMap[A: TypeTag](g: R => A, meta: Option[Any] = None): GenExtractor[T, A] = {
    andEx(g, meta)
  }

  def andFlatMap[A: TypeTag](g: R => Option[A], meta: Option[Any] = None): GenExtractor[T, A] = {
    andEx(GenExtractor.fromOptionFn(g), meta)
  }

  def andTyped[R2 >: R, A](
      g: R2 => A,
      poly: DataType => DataType,
      meta: Option[Any] = None
  ): GenExtractor[T, A] = andEx(
    fromPoly[R2, A]()(poly, g),
    meta
  )

  def andOptionTyped[R2 >: R, A](
      g: R2 => Option[A],
      poly: DataType => DataType,
      meta: Option[Any] = None
  ): GenExtractor[T, A] = andTyped(Unlift(g), poly, meta)

  def cast[A]: GenExtractor[T, A] = {

    this.asInstanceOf[GenExtractor[T, A]]
  }

  def filterByType[A: TypeTag]: GenExtractor[T, A] = {
    implicit val ctg: ClassTag[A] = TypeMagnet.FromTypeTag[A].asClassTag

    andFlatMap[A] {
      SpookyUtils.typedOrNone[A]
    }
  }

  def toStr: GenExtractor[T, String] = andMap(_.toString)
}
