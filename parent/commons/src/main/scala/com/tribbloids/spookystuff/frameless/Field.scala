package com.tribbloids.spookystuff.frameless

import ai.acyclic.prover.commons.cap.Capability.{<>, Universe}
import ai.acyclic.prover.commons.function.hom.Hom
import com.tribbloids.spookystuff.frameless.Tuple.Empty
import shapeless.ops.record.MapValues

import scala.language.implicitConversions

object Field extends Universe {
  // ... is a compile-time-only construct

  trait Named[K <: XStr, V] extends Capability {
    self: V =>
  }
  // TODO: can this be merged into K := V ? to maintain shapeless compatibility?

  object Named {

    class AnnotatorByApply[K <: XStr] {

      def apply[V](v: V): V <> Named[K, V] = {
        v.asInstanceOf[V <> Named[K, V]]
      }
    }

    def apply[K <: XStr] = new AnnotatorByApply[K]

    def apply[K <: XStr](name: K) = new AnnotatorByApply[name.type]

    case class AsTypedRowView[K <: XStr, V](self: V <> Named[K, V]) extends TypedRow.ElementView[T1[K := V]] {

      override def asTypeRow: TypedRow[(K := V) *: Empty] = {

        TypedRowInternal.ofElement(named[K] := self.asInstanceOf[V])
      }
    }

    implicit def asTypedRowView[K <: XStr, V](self: V <> Named[K, V]): AsTypedRowView[K, V] =
      AsTypedRowView[K, V](self)
  }

  object CanSort extends Capability {

    import shapeless.record._

    def apply[V](v: V) = {
      v <>: CanSort
    }

    def apply[L <: Tuple](typedRow: TypedRow[L])(
        implicit
        ev: MapValues[Enable.asShapelessPoly1.type, L]
    ): TypedRow[ev.Out] = {

      val mapped = typedRow._internal.repr.mapValues(Enable)(ev)

      TypedRowInternal.ofTuple(mapped)
    }

    object Enable extends Hom.Poly {

      implicit def only[T]: T Target (T <> CanSort.type) = at[T] { v =>
        v.asInstanceOf[T <> CanSort.type]
      }
    }
  }

  /**
    * define whether to evict old values that has identical field name in previous table
    */
  sealed abstract class ConflictResolving extends Capability
  // TODO: it is useless right now, precedence of fields in merging are totally determined by whether to use ++< or >++
  //  may be enabled later for fine-grained control

  // Fail fast
  object Error extends ConflictResolving

  // Always evict old value
  object Replace extends ConflictResolving

  // Only evict old value if the new value is not NULL.
  object ReplaceIfNotNull extends ConflictResolving
}
