package com.tribbloids.spookystuff.linq

import ai.acyclic.prover.commons.function.hom.Hom
import com.tribbloids.spookystuff.linq.Linq.Row
import com.tribbloids.spookystuff.linq.internal.TupleOrdering
import shapeless.ops.hlist.Mapper

class RowOrdering {

  trait Impl[T <: Tuple] extends Ordering[Row[T]]
}

object RowOrdering {

  import Field.*

  object Default extends RowOrdering {

    trait By_Imp0 extends Hom.Poly {

      implicit def ignore[T]: T Target Unit = this.at[T] { (_: T) =>
        ()
      }
    }

    object By extends By_Imp0 {

      implicit def accept[T <: ? <> CanSort.type]: T Target T = this.at[T] { (v: T) =>
        v
      }
    }

    def at[R <: Tuple] = new At[R]
    class At[R <: Tuple] {

      // applicable to all cases of Tuple, even without KeyTags
      case class Factory[
          MO <: Tuple
      ]()(
          implicit
          mapper: Mapper.Aux[By.asShapelessPoly1.type, R, MO]
      ) {

        lazy val fn: Row[R] => MO = { (row: Row[R]) =>
          val mapped = mapper.apply(row._internal.repr)

          mapped
        }

        def get(
            implicit
            delegate: TupleOrdering.Native.Impl[MO]
        ): Impl[R] = {

          new Impl[R] {
            override def compare(x: Row[R], y: Row[R]): Int = {
              delegate.compare(fn(x), fn(y))
            }
          }
        }
      }
    }

    trait Giver {

      implicit def _ordering[
          R <: Tuple,
          MO <: Tuple
      ](
          implicit
          mapper: Mapper.Aux[By.asShapelessPoly1.type, R, MO],
          delegate: TupleOrdering.Native.Impl[MO]
      ): Impl[R] = at[R].Factory().get
    }
  }
}
