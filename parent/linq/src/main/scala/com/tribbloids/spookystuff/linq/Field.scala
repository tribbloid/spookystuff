package com.tribbloids.spookystuff.linq

import ai.acyclic.prover.commons.cap.Capability
import ai.acyclic.prover.commons.cap.Capability.Universe
import ai.acyclic.prover.commons.function.hom.Hom
import com.tribbloids.spookystuff.linq.Linq.{Cell, Row}
import com.tribbloids.spookystuff.linq.internal.RowInternal
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

    implicit def asCell[K <: XStr, V](self: V <> Named[K, V]): Cell[K, V] = Cell(self)
  }

  object CanSort extends Capability {

    import shapeless.record._
    def apply[V](v: V): V <> CanSort.type = {
      v <>: CanSort
    }

    def apply[L <: Tuple](row: Row[L])(
        implicit
        ev: MapValues[Enable.asShapelessPoly1.type, L]
    ): Row[ev.Out] = {

      val mapped = row._internal.repr.mapValues(Enable)(ev)

      RowInternal.ofTuple(mapped)
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
