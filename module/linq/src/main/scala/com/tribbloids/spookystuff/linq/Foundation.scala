package com.tribbloids.spookystuff.linq

import ai.acyclic.prover.commons.compat.NamedTupleX.:=
import ai.acyclic.prover.commons.compat.TupleX.{T0, T1}
import ai.acyclic.prover.commons.compat.{Key, TupleX, XStr}
import com.tribbloids.spookystuff.linq.Rec
import com.tribbloids.spookystuff.linq.catalyst.{RowEncoder, RowEncoderStage1}
import com.tribbloids.spookystuff.linq.internal.ElementWisePoly
import frameless.TypedEncoder
import shapeless.RecordArgs

import scala.reflect.ClassTag

object Foundation extends RowOrdering.Default.Giver {

  sealed trait KVBatchLike[T <: TupleX] {}

  object KVBatchLike {

    implicit class TaggedValueAsCell[K <: XStr, V](self: K := V) extends CellLike[T1[K := V]] {
      lazy val asRow: Rec[T1[K := V]] = Rec.ofTuple((Key[K] := self) *: T0)
    }
  }

  def unbox[T <: TupleX](v: KVBatchLike[T]): Seq[Rec[T]] = v match {
    case v: KVBatch[T] => v.rows
    case v: KVPairs[T] => Seq(KVPairs.unbox(v))
  }

  sealed trait KVPairs[T <: TupleX] extends KVBatchLike[T] {}

  object KVPairs {

    def unbox[T <: TupleX](v: KVPairs[T]): Rec[T] = v match {
      case v: CellLike[T] => v.asRow
      case v: Rec[T]   => v
    }
  }

  private[linq] trait CellLike[T <: TupleX] extends KVPairs[T] {
    // can also be used as an operand in merge, like Seq[TypedRow[T]]

    def asRow: Rec[T]
  }

  private[linq] trait RecordLike[T <: TupleX] extends KVPairs[T] with Foundation.LeftOpsMixin[T] {
    // TODO: why do I need this? shoud all be in the Record

    // merge (++) method can be called directly on it
    // plz avoid introducing too much protected/public member as it corrupts TypedRow selector

    private val self: Rec[T] = KVPairs.unbox(this)

    @transient lazy val +<+ : ElementWisePoly.preferRight.MergeMethod[T] =
      ElementWisePoly.preferRight.MergeMethod(self)

    @transient lazy val +>+ : ElementWisePoly.preferLeft.MergeMethod[T] =
      ElementWisePoly.preferLeft.MergeMethod(self)

    @transient lazy val +!+ : ElementWisePoly.ifNoConflict.MergeMethod[T] =
      ElementWisePoly.ifNoConflict.MergeMethod(self)

    def ++ : ElementWisePoly.preferRight.MergeMethod[T] = +<+ // default

    object update extends RecordArgs {

      def applyRecord[R <: TupleX](list: R)(
          implicit
          lemma: ElementWisePoly.preferRight.LemmaAtRows[T, R]
      ): lemma.Out = {

        val neo: Rec[R] = Rec.ofTuple(list)
        val result: lemma.Out = +<+(neo)
        result
      }
    }

    object updateIfNotExists extends RecordArgs {

      def applyRecord[R <: TupleX](list: R)(
          implicit
          lemma: ElementWisePoly.preferLeft.LemmaAtRows[T, R]
      ): lemma.Out = {

        val neo: Rec[R] = Rec.ofTuple(list)
        val result: lemma.Out = +>+(neo)
        result
      }
    }

    object updateIfNoConflict extends RecordArgs {

      def applyRecord[R <: TupleX](list: R)(
          implicit
          lemma: ElementWisePoly.ifNoConflict.LemmaAtRows[T, R]
      ): lemma.Out = {

        val neo: Rec[R] = Rec.ofTuple(list)
        val result: lemma.Out = +!+(neo)
        result
      }
    }

//    object append {
//
//      def apply
//    }

    object append {

      def apply[V, R](v: V)(
        implicit
        lemma1: Rec.ofData.Lemma[V, R],
        lemma2: ElementWisePoly.ifNoConflict.Lemma.At[(Rec[T], R)]
      ): lemma2.Out = {

        val right: R = lemma1.apply(v)
        val result = lemma2((self -> right): (Rec[T], R))

        result
      }
    }
  }

  lazy val empty: Rec[TupleX.T0] = Rec.ofTuple(TupleX.T0)

  // TODO: should be %, as in record4s
  object ^ extends RecordArgs {

    def applyRecord[L <: TupleX](list: L): Rec[L] = Rec.ofTuple(list)
  }

  implicit def _getEncoder[G <: TupleX](
      implicit
      stage1: RowEncoderStage1[G, G],
      classTag: ClassTag[Rec[G]]
  ): TypedEncoder[Rec[G]] = RowEncoder.^[G, G]()

  trait LeftOpsMixin[T <: TupleX] {
    raw: KVBatchLike[T] =>
    // Cartesian product (><) method can be called directly on it
    // plz avoid introducing too much protected/public member as it corrupts TypedRow selector

    private val self: Seq[Rec[T]] = Foundation.unbox(this)

    @transient lazy val ><< : ElementWisePoly.preferRight.CartesianProductMethod[T] =
      ElementWisePoly.preferRight.CartesianProductMethod(self)

    @transient lazy val >>< : ElementWisePoly.preferLeft.CartesianProductMethod[T] =
      ElementWisePoly.preferLeft.CartesianProductMethod(self)

    @transient lazy val >!< : ElementWisePoly.ifNoConflict.CartesianProductMethod[T] =
      ElementWisePoly.ifNoConflict.CartesianProductMethod(self)

    def >< : ElementWisePoly.preferRight.CartesianProductMethod[T] = ><< // default
  }

  trait KVBatch[T <: TupleX] extends KVBatchLike[T] {
    // can be used as operand in Cartesian product, like Seq[TypedRow[T]]

    def rows: Seq[Rec[T]]
  }

//  trait LeftElementView[T <: Tuple] extends LeftElementAPI[T] with ElementView[T] with LeftSeqView[T] {} // TOOD: remove, useless

  @transient lazy val functions: RowFunctions.type = RowFunctions
}
