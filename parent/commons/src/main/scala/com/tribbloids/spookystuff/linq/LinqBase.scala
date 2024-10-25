package com.tribbloids.spookystuff.linq

import com.tribbloids.spookystuff.linq.Linq.Row
import com.tribbloids.spookystuff.linq.catalyst.{RowEncoder, RowEncoderStage1}
import com.tribbloids.spookystuff.linq.internal.{ElementWisePoly, RowInternal}
import frameless.TypedEncoder
import shapeless.RecordArgs

import scala.language.{dynamics, implicitConversions}
import scala.reflect.ClassTag

object LinqBase extends RowOrdering.Default.Giver {

  sealed trait Batch[T <: Tuple] {}

  def unbox[T <: Tuple](v: Batch[T]): Seq[Row[T]] = v match {
    case v: BatchView[T] => v.asBatch
    case v: Entry[T]     => Seq(Entry.unbox(v))
  }

  sealed trait Entry[T <: Tuple] extends Batch[T] {}

  object Entry {

    def unbox[T <: Tuple](v: Entry[T]): Row[T] = v match {
      case v: CellLike[T] => v.asRow
      case v: Row[T]      => v
    }
  }

  private[linq] trait CellLike[T <: Tuple] extends Entry[T] {
    // can also be used as an operand in merge, like Seq[TypedRow[T]]

    def asRow: Row[T]
  }

  private[linq] trait RowLike[T <: Tuple] extends Entry[T] with LinqBase.LeftOpsMixin[T] {
    // merge (++) method can be called directly on it
    // plz avoid introducing too much protected/public member as it corrupts TypedRow selector

    private val self: Row[T] = Entry.unbox(this)

    @transient lazy val +<+ : ElementWisePoly.preferRight.MergeMethod[T] =
      ElementWisePoly.preferRight.MergeMethod(self)

    @transient lazy val +>+ : ElementWisePoly.preferLeft.MergeMethod[T] =
      ElementWisePoly.preferLeft.MergeMethod(self)

    @transient lazy val +!+ : ElementWisePoly.requireNoConflict.MergeMethod[T] =
      ElementWisePoly.requireNoConflict.MergeMethod(self)

    def ++ : ElementWisePoly.preferRight.MergeMethod[T] = +<+ // default

    object update extends RecordArgs {

      def applyRecord[R <: Tuple](list: R)(
          implicit
          lemma: ElementWisePoly.preferRight.LemmaAtRows[T, R]
      ): lemma.Out = {

        val neo: Row[R] = RowInternal.ofTuple(list)
        val result: lemma.Out = +<+(neo)
        result
      }
    }
  }

  lazy val empty: Row[Tuple.Empty] = RowInternal.ofTuple(Tuple.empty)

  // TODO: should be %, as in record4s
  object ^ extends RecordArgs {

    def applyRecord[L <: Tuple](list: L): Row[L] = RowInternal.ofTuple(list)
  }

  implicit def _getEncoder[G <: Tuple](
      implicit
      stage1: RowEncoderStage1[G, G],
      classTag: ClassTag[Row[G]]
  ): TypedEncoder[Row[G]] = RowEncoder.^[G, G]()

  trait LeftOpsMixin[T <: Tuple] {
    raw: Batch[T] =>
    // Cartesian product (><) method can be called directly on it
    // plz avoid introducing too much protected/public member as it corrupts TypedRow selector

    private val self: Seq[Row[T]] = LinqBase.unbox(this)

    @transient lazy val ><< : ElementWisePoly.preferRight.CartesianProductMethod[T] =
      ElementWisePoly.preferRight.CartesianProductMethod(self)

    @transient lazy val >>< : ElementWisePoly.preferLeft.CartesianProductMethod[T] =
      ElementWisePoly.preferLeft.CartesianProductMethod(self)

    @transient lazy val >!< : ElementWisePoly.requireNoConflict.CartesianProductMethod[T] =
      ElementWisePoly.requireNoConflict.CartesianProductMethod(self)

    def >< : ElementWisePoly.preferRight.CartesianProductMethod[T] = ><< // default
  }

  trait BatchView[T <: Tuple] extends Batch[T] {
    // can be used as operand in Cartesian product, like Seq[TypedRow[T]]

    def asBatch: Seq[Row[T]]
  }

//  trait LeftElementView[T <: Tuple] extends LeftElementAPI[T] with ElementView[T] with LeftSeqView[T] {} // TOOD: remove, useless

  @transient lazy val functions: RowFunctions.type = RowFunctions
}
