package com.tribbloids.spookystuff.frameless

import com.tribbloids.spookystuff.frameless.TypedRowInternal.Merge
import frameless.TypedEncoder
import shapeless.RecordArgs

import scala.collection.immutable.ArraySeq
import scala.language.dynamics
import scala.reflect.ClassTag

/**
  * shapeless Tuple & Record has high runtime overhead and poor Scala 3 compatibility. its usage should be minimized
  *
  * do not use shapeless instances for data storage/shipping
  *
  * @param cells
  *   data
  * @tparam L
  *   Record type
  */
final class TypedRow[L <: Tuple](
    cells: ArraySeq[Any]
) extends Dynamic {
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
      selector: Selector[L, Col[key.type]]
  ): selector.Out = {

    _fields.selectDynamic(key).value
  }

  @transient override lazy val toString: String = cells.mkString("[", ",", "]")

  sealed class FieldView[K, V](
      val key: K
  )(
      val selector: Selector.Aux[L, K, V]
  ) {

    lazy val valueWithField: V = selector(_internal.repr)

    lazy val value: V = {
      valueWithField // TODO: should remove Field capability mixins
    }

    lazy val asTypedRow: TypedRow[(K ->> V) *: Tuple.Empty] = {
      TypedRowInternal.ofTuple(->>[K](valueWithField) *: Tuple.Empty)
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
          ev: Merge.keepRight.Theorem[L, (K ->> VV) *: Tuple.Empty]
      ): ev.Out = {

        val neo: TypedRow[(K ->> VV) *: Tuple.Empty] = TypedRowInternal.ofTuple(->>[K](value) *: Tuple.Empty)

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
        selector: Selector[L, Col[key.type]]
    ) = new FieldView[Col[key.type], selector.Out](Col(key))(selector)
  }

  @transient lazy val _internal: TypedRowInternal[L] = {

    TypedRowInternal(cells)
  }

  def ++< = TypedRowInternal.Merge.keepRight.Curried(this)

  def >++ = TypedRowInternal.Merge.keepLeft.Curried(this)

  def ++ = TypedRowInternal.Merge.rigorous.Curried(this)

  object update extends RecordArgs {

    // TODO: call ++<
  }
}

object TypedRow extends TypedRowOrdering.Default.Giver with RecordArgs {

  def applyRecord[L <: Tuple](list: L): TypedRow[L] = TypedRowInternal.ofTuple(list)

  implicit def _getEncoder[G <: Tuple](
      implicit
      stage1: RecordEncoderStage1[G, G],
      classTag: ClassTag[TypedRow[G]]
  ): TypedEncoder[TypedRow[G]] = RecordEncoder.ForTypedRow[G, G]()

}
