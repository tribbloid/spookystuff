package com.tribbloids.spookystuff.linq

import ai.acyclic.prover.commons.compat.NamedTupleX.:=
import ai.acyclic.prover.commons.compat.TupleX.{*:, T0}
import ai.acyclic.prover.commons.compat.{Key, TupleX, XStr}
import ai.acyclic.prover.commons.function.hom.Hom
import com.tribbloids.spookystuff.linq
import com.tribbloids.spookystuff.linq.Foundation.{KVBatch, RecordLike}
import com.tribbloids.spookystuff.linq.internal.RecInternal
import shapeless.RecordArgs

import scala.language.dynamics

object Rec extends RecordArgs {

  def applyRecord[L <: TupleX](list: L): Rec[L] = Rec.ofTupleX(list)

  def ofTupleX[L <: TupleX](
      data: L
  ): linq.Rec[L] = {

    val cells = data.runtimeList

    new linq.Rec[L](cells.to(Vector))
  }

  def ofTagged[K <: XStr, V](
      v: K := V
  ): linq.Rec[(K := V) *: T0] = ofTupleX(v *: TupleX.T0)

  sealed protected trait OfData_Imp0 extends Hom.Poly {

    implicit def fromValue[V]: V |- linq.Rec[("value" := V) *: TupleX.T0] = at[V] { v =>
      ofTupleX((Key["value"] := v) *: TupleX.T0)
    }
  }

  sealed protected trait OfData_Imp1 extends OfData_Imp0 {

    // implicit def fromProduct[L <: Product] = {
    //   ??? // TODO: impl after TupleX and Product/Tuple has been unified in Scala 3
    // }
  }

  object ofData extends OfData_Imp1 {
    // can be used to convert value of any type into a Record

    implicit def id[L <: TupleX]: linq.Rec[L] |- linq.Rec[L] = at[linq.Rec[L]] {
      identity[linq.Rec[L]]
    }
  }

  implicit class _rowSeqView[T <: TupleX](
      val rows: Seq[Rec[T]]
  ) extends Foundation.LeftOpsMixin[T]
      with KVBatch[T] {

    // cartesian product can be directly called on Seq
  }
}

/**
  * shapeless Tuple & Record has high runtime overhead and poor Scala 3 compatibility. its usage should be minimized
  *
  * do not use shapeless instances for data storage/shipping
  *
  * @param runtimeData
  *   data
  * @tparam T
  *   Record type
  */
final class Rec[T <: TupleX](
    runtimeData: Vector[Any] // TODO: should use unboxed binary data structure, Java 21 or Apache Arrow maybe helpful
    // TODO: there should be a few 3rd party libraries in Scala 3
) extends Dynamic
    with RecordLike[T] {

  // TODO: how to easily reconstruct vertices/edges for graphX/graphframe?
  //  since graphframe table always demand id/src/tgt columns, should the default
  //  representation be SemiRow? that contains both structured and newType part?

  import shapeless.ops.record.*

  /**
    * Allows dynamic-style access to fields of the record whose keys are Symbols. See
    * [[shapeless.syntax.DynamicRecordOps[_]] for original version
    *
    * CAUTION: this takes all the slots for nullary fields, none the following functions will be nullary
    */
  def selectDynamic(key: XStr)(
      implicit
      selector: Selector[T, Key.Tag[key.type]]
      //        remover: Remover[T, key.type]
  ): key.type := selector.Out = {

    val value: selector.Out = FieldAccessor(this).selectDynamic(key).value

    Key[key.type] := value

    //      Field.Named[key.type].apply(value: selector.Out)

  }

  @transient override lazy val toString: String = runtimeData.mkString("[", ",", "]")

//  object _fields extends Dynamic {
//
//    def selectDynamic(key: XStr)(
//        implicit
//        selector: Selector[T, Key.Tag[key.type]]
//        //          remover: Remover[T, key.type]
//    ) = new FieldSelection[key.type, selector.Out]()(selector)
//  }

  @transient lazy val _internal: RecInternal[T] = {

    RecInternal(runtimeData)
  }

}
