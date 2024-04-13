package com.tribbloids.spookystuff.frameless

import com.tribbloids.spookystuff.frameless.TypedRowInternal.ElementWiseMethods
import frameless.TypedEncoder
import shapeless.RecordArgs

import scala.language.{dynamics, implicitConversions}
import scala.reflect.ClassTag

/**
  * shapeless Tuple & Record has high runtime overhead and poor Scala 3 compatibility. its usage should be minimized
  *
  * do not use shapeless instances for data storage/shipping
  *
  * @param runtimeVector
  *   data
  * @tparam T
  *   Record type
  */
final class TypedRow[T <: Tuple](
    runtimeVector: Vector[Any]
) extends Dynamic
    with TypedRow.LeftElementAPI[T] {

  // TODO: should be "NamedTuple"

  // TODO: how to easily reconstruct vertices/edges for graphX/graphframe?
  //  since graphframe table always demand id/src/tgt columns, should the default
  //  representation be SemiRow? that contains both structured and newType part?

  import shapeless.ops.record._

  /**
    * Allows dynamic-style access to fields of the record whose keys are Symbols. See
    * [[shapeless.syntax.DynamicRecordOps[_]] for original version
    *
    * CAUTION: this takes all the slots for nullary fields, none the following functions will be nullary
    */
  def selectDynamic(key: XStr)(
      implicit
      selector: Selector[T, Col[key.type]]
  ) = {

    val value: selector.Out = _fields.selectDynamic(key).value

    Field.Named.apply[key.type](value: selector.Out)

  }

  @transient override lazy val toString: String = runtimeVector.mkString("[", ",", "]")

  // TODO: remove, don't use, Field.Name mixin is good enough!
  sealed class FieldView[K, V](
      val key: K
  )(
      val selector: Selector.Aux[T, K, V]
  ) {

    lazy val valueWithField: V = selector(_internal.repr)

    lazy val value: V = {
      valueWithField // TODO: should remove Field capability mixins
    }

    lazy val asTypedRow: TypedRow[(K ->> V) *: Tuple.Empty] = {
      TypedRowInternal.ofTuple(->>[K](valueWithField) *: Tuple.empty)
    }

    //    lazy val value: V = selector(asRepr)

    //    object remove {
    //
    //      def apply[L2 <: Tuple, O2 <: Tuple]()(
    //          implicit
    //          ev: Remover.Aux[L, K, (Any, O2)]
    //      ): TypedRow[O2] = {
    //
    ////        val tuple = repr.remove(key)(ev)
    //        val tuple = ev.apply(repr)
    //
    //        TypedRowInternal.ofTuple(tuple._2)
    //      }
    //    }
    //    def - : remove.type = remove

    object set {

      def apply[VV](value: VV)(
          implicit
          ev: ElementWiseMethods.preferRight.Theorem[T, (K ->> VV) *: Tuple.Empty]
      ): ev.Out = {

        val neo: TypedRow[(K ->> VV) *: Tuple.Empty] = TypedRowInternal.ofTuple(->>[K](value) *: Tuple.empty)

        val result = ev.apply(TypedRow.this -> neo)

        result
      }
    }

    def := : set.type = set

    /**
      * To be used in [[org.apache.spark.sql.Dataset]].flatMap
      */

//    def explode[R](
//        fn: V => Seq[R]
//    )(
//        implicit
//        ev1: Merge.keepRight.Theorem[L, (K ->> R) *: Tuple.Empty]
//    ): Seq[ev1.Out] = {
//
//      val results = valueWithField.map { v: V =>
//        val r = fn(v)
//        set(r)(ev1)
//      }
//      results
//    }

  }

  object _fields extends Dynamic {

    def selectDynamic(key: XStr)(
        implicit
        selector: Selector[T, Col[key.type]]
    ) = new FieldView[Col[key.type], selector.Out](Col(key))(selector)
  }

  @transient lazy val _internal: TypedRowInternal[T] = {

    TypedRowInternal(runtimeVector)
  }

}

object TypedRow extends TypedRowOrdering.Default.Giver {

  lazy val empty: TypedRow[Tuple.Empty] = TypedRowInternal.ofTuple(Tuple.empty)

  object ^ extends RecordArgs {

    def applyRecord[L <: Tuple](list: L): TypedRow[L] = TypedRowInternal.ofTuple(list)
  }

  implicit def _getEncoder[G <: Tuple](
      implicit
      stage1: RecordEncoderStage1[G, G],
      classTag: ClassTag[TypedRow[G]]
  ): TypedEncoder[TypedRow[G]] = RecordEncoder.ForTypedRow[G, G]()

  sealed trait SeqAPI[T <: Tuple] {}

  object SeqAPI {

    def unbox[T <: Tuple](v: SeqAPI[T]): Seq[TypedRow[T]] = v match {
      case v: SeqView[T]  => v.asTypeRowSeq
      case v: TypedRow[T] => Seq(v)
    }
  }

  trait SeqView[T <: Tuple] extends SeqAPI[T] {
    // can be used as operand in Cartesian product, like Seq[TypedRow[T]]

    def asTypeRowSeq: Seq[TypedRow[T]]
  }

  sealed trait LeftSeqAPI[T <: Tuple] extends SeqAPI[T] {
    // Cartesian product (><) method can be called directly on it
    // plz avoid introducing too much protected/public member as it corrupts TypedRow selector

    private val self: Seq[TypedRow[T]] = SeqAPI.unbox(this)

    @transient lazy val ><< : ElementWiseMethods.preferRight.CartesianProductMethod[T] =
      TypedRowInternal.ElementWiseMethods.preferRight.CartesianProductMethod(self)

    @transient lazy val >>< : ElementWiseMethods.preferLeft.CartesianProductMethod[T] =
      TypedRowInternal.ElementWiseMethods.preferLeft.CartesianProductMethod(self)

    @transient lazy val >!< : ElementWiseMethods.requireNoConflict.CartesianProductMethod[T] =
      TypedRowInternal.ElementWiseMethods.requireNoConflict.CartesianProductMethod(self)

    def >< : ElementWiseMethods.preferRight.CartesianProductMethod[T] = ><< // default
  }

  case class LeftSeqView[T <: Tuple](
      asTypeRowSeq: Seq[TypedRow[T]]
  ) extends LeftSeqAPI[T]
      with SeqView[T] {

    // cartesian product can be directly called on Seq
  }

  implicit def leftSeqView[T <: Tuple](self: Seq[TypedRow[T]]): LeftSeqView[T] = LeftSeqView(self)

  sealed trait ElementAPI[T <: Tuple] extends SeqAPI[T] {}

  object ElementAPI {

    def unbox[T <: Tuple](v: ElementAPI[T]): TypedRow[T] = v match {
      case v: ElementView[T] => v.asTypeRow
      case v: TypedRow[T]    => v
    }
  }

  trait ElementView[T <: Tuple] extends ElementAPI[T] with SeqView[T] {
    // can also be used as an operand in merge, like Seq[TypedRow[T]]

    def asTypeRow: TypedRow[T]

    final def asTypeRowSeq: Seq[TypedRow[T]] = Seq(asTypeRow)
  }

  sealed trait LeftElementAPI[T <: Tuple] extends ElementAPI[T] with LeftSeqAPI[T] {
    // merge (++) method can be called directly on it
    // plz avoid introducing too much protected/public member as it corrupts TypedRow selector

    private val self: TypedRow[T] = ElementAPI.unbox(this)

    @transient lazy val +<+ : ElementWiseMethods.preferRight.MergeMethod[T] =
      TypedRowInternal.ElementWiseMethods.preferRight.MergeMethod(self)

    @transient lazy val +>+ : ElementWiseMethods.preferLeft.MergeMethod[T] =
      TypedRowInternal.ElementWiseMethods.preferLeft.MergeMethod(self)

    @transient lazy val +!+ : ElementWiseMethods.requireNoConflict.MergeMethod[T] =
      TypedRowInternal.ElementWiseMethods.requireNoConflict.MergeMethod(self)

    def ++ : ElementWiseMethods.preferRight.MergeMethod[T] = +<+ // default

    object update extends RecordArgs {

      def applyRecord[R <: Tuple](list: R)(
          implicit
          lemma: TypedRowInternal.ElementWiseMethods.preferRight.Theorem[T, R]
      ) = {

        val neo = TypedRowInternal.ofTuple(list)
        +<+(neo)
      }
    }
  }

//  trait LeftElementView[T <: Tuple] extends LeftElementAPI[T] with ElementView[T] with LeftSeqView[T] {} // TOOD: remove, useless

  @transient lazy val functions: TypedRowFunctions.type = TypedRowFunctions
}
