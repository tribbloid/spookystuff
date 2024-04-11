package com.tribbloids.spookystuff.frameless

import ai.acyclic.prover.commons.function.Hom
import ai.acyclic.prover.commons.util.Capabilities
import com.tribbloids.spookystuff.frameless.Tuple.Empty
import shapeless.ops.record.MapValues

import scala.language.implicitConversions

object Field extends Capabilities {
  // ... is a compile-time-only construct

  class Name[K <: XStr]() extends Capability

  object Name {

    def apply[K <: XStr]: Name[K] = new Name[K]()

    def apply[K <: XStr](name: K) = new Name[name.type]()

    case class AsTypedRowView[K <: XStr, V](self: V ^: Name[K])
        extends TypedRow.ElementView[Col_->>[K, V] *: Tuple.Empty] {

      override def asTypeRow: TypedRow[Col_->>[K, V] *: Empty] = {

        TypedRowInternal.ofElement(col[K] ->> self.asInstanceOf[V])
      }
    }

    implicit def asTypedRowView[K <: XStr, V](self: V ^: Name[K]): AsTypedRowView[K, V] = AsTypedRowView(self)
  }

  object CanSort extends Capability {

    import shapeless.record._

    def apply[V](v: V) = {
      v ^: CanSort
    }

    def apply[L <: Tuple](typedRow: TypedRow[L])(
        implicit
        ev: MapValues[Enable.asShapeless.type, L]
    ): TypedRow[ev.Out] = {

      val mapped = typedRow._internal.repr.mapValues(Enable)(ev)

      TypedRowInternal.ofTuple(mapped)
    }

    object Enable extends Hom.Poly {

      implicit def only[T]: T =>> (T ^: CanSort.type) = at[T] { v =>
        v.asInstanceOf[T ^: CanSort.type]
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
