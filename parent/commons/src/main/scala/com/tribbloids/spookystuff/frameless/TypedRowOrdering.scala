package com.tribbloids.spookystuff.frameless

import ai.acyclic.prover.commons.function.Hom
import shapeless.ops.hlist.Mapper

class TypedRowOrdering {

  trait Impl[T <: Tuple] extends Ordering[TypedRow[T]]
}

object TypedRowOrdering {

  import Field._

  object Default extends TypedRowOrdering {

    trait By_Imp0 extends Hom.Poly {

      implicit def ignore[T]: T =>> Unit = at[T] { _: T =>
        ()
      }
    }

    object By extends By_Imp0 {

      implicit def accept[T <: _ ^^ CanSort]: T =>> T = at[T] { v: T =>
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
          mapper: Mapper.Aux[By.asShapeless.type, R, MO]
      ) {

        lazy val fn: TypedRow[R] => MO = { row: TypedRow[R] =>
          val mapped = mapper.apply(row.repr)

          mapped
        }

        def get(
            implicit
            delegate: TupleOrdering.Native.Impl[MO]
        ): Impl[R] = {

          new Impl[R] {
            override def compare(x: TypedRow[R], y: TypedRow[R]): Int = {
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
          mapper: Mapper.Aux[By.asShapeless.type, R, MO],
          delegate: TupleOrdering.Native.Impl[MO]
      ): Impl[R] = at[R].Factory().get
    }
  }
}
