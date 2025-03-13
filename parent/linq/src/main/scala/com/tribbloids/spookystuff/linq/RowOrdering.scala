package com.tribbloids.spookystuff.linq

import ai.acyclic.prover.commons.cap.Capability.<>
import ai.acyclic.prover.commons.compat.{TupleX, TupleXOrdering}
import ai.acyclic.prover.commons.function.hom.Hom
import com.tribbloids.spookystuff.linq.Linq.Row
import shapeless.ops.hlist.Mapper

class RowOrdering {

  trait Impl[T <: TupleX] extends Ordering[Row[T]]
}

object RowOrdering {

  import Field.*

  object Default extends RowOrdering {

    trait By_Imp0 extends Hom.Poly {

      implicit def ignore[T]: T |- Unit = this.at[T] { (_: T) =>
        ()
      }
    }

    object By extends By_Imp0 {

      implicit def accept[T <: ? <> CanSort.type]: T |- T = this.at[T] { (v: T) =>
        v
      }
    }

    def at[R <: TupleX] = new At[R]
    class At[R <: TupleX] {

      // applicable to all cases of Tuple, even without KeyTags
      case class Factory[
          MO <: TupleX
      ]()(
          implicit
          mapper: Mapper.Aux[By.asTupleMapper.type, R, MO]
      ) {

        lazy val fn: Row[R] => MO = { (row: Row[R]) =>
          val mapped = mapper.apply(row._internal.repr)

          mapped
        }

        def get(
            implicit
            delegate: TupleXOrdering.Native.Impl[MO]
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
          R <: TupleX,
          MO <: TupleX
      ](
          implicit
          mapper: Mapper.Aux[By.asTupleMapper.type, R, MO],
          delegate: TupleXOrdering.Native.Impl[MO]
      ): Impl[R] = at[R].Factory().get
    }
  }
}
