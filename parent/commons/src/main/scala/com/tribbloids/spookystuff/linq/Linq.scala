package com.tribbloids.spookystuff.linq

import ai.acyclic.prover.commons.cap.Capability.<>
import com.tribbloids.spookystuff.linq.LinqBase.{BatchView, CellLike, RowLike}
import com.tribbloids.spookystuff.linq.Tuple.Empty
import com.tribbloids.spookystuff.linq.internal.{ElementWisePoly, RowInternal}
import shapeless.labelled
import shapeless.labelled.{FieldType, field}
import shapeless.tag.@@

object Linq {

  import scala.language.dynamics

  case class Cell[K <: XStr, V](self: V) extends CellLike[T1[K := V]] {

    override def asRow: Row[(K := V) *: Empty] = {

      RowInternal.ofElement(named[K] := self.asInstanceOf[V])
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
  final class Row[T <: Tuple](
      runtimeData: Vector[Any] // TODO: should use unboxed binary data structure, Java 21 or Apache Arrow maybe helpful
  ) extends Dynamic
      with RowLike[T] {

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
    ): selector.Out <> Field.Named[key.type, selector.Out] = {

      val value: selector.Out = _fields.selectDynamic(key).value

      Field.Named.apply[key.type](value: selector.Out)

    }

    @transient override lazy val toString: String = runtimeData.mkString("[", ",", "]")

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

      lazy val asRow: Row[(K ->> V) *: Tuple.Empty] = {
        RowInternal.ofTuple(->>[K](valueWithField) *: Tuple.empty)
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
            ev: ElementWisePoly.preferRight.LemmaAtRows[T, (K ->> VV) *: Tuple.Empty]
        ): ev.Out = {

          val neo: Row[(K ->> VV) *: Tuple.Empty] = RowInternal.ofTuple(->>[K](value) *: Tuple.empty)

          val result = ev.apply(Row.this -> neo)

          result.asInstanceOf[ev.Out]
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

    @transient lazy val _internal: RowInternal[T] = {

      RowInternal(runtimeData)
    }

  }

  implicit class _SeqExtensions[T <: Tuple](
      val asBatch: Seq[Row[T]]
  ) extends LinqBase.LeftOpsMixin[T]
      with BatchView[T] {

    // cartesian product can be directly called on Seq
  }

  // --------------------------------------------------------------------------------------------------------------------

  type ->>[K, V] = FieldType[K, V]

  def ->>[K]: labelled.FieldBuilder[K] = field[K]

  // TODO: the following definition for Col will be obsolete in shapeless 2.4
  //  upgrade blocked by frameless

  type Col[T <: XStr] = Symbol @@ T

  def Col[T <: XStr](v: T): Col[T] = {

    Symbol(v).asInstanceOf[Col[T]]
  }

  type :=[K <: XStr, V] = Col[K] ->> V

  class NamedValueConstructor[K <: XStr]() {

    def :=[V](v: V): K := v.type = {
      v.asInstanceOf[K := v.type]
    }
  }
  def named[K <: XStr] = new NamedValueConstructor[K]()

}
