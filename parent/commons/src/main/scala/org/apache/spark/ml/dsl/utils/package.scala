package org.apache.spark.ml.dsl

import org.apache.spark.ml.dsl.utils.NullSafeMagnet.Cap

package object utils {

  type ??[T, M <: Cap] = NullSafeMagnet.CanBeNull[T, M]

  type !![T, M <: Cap] = NullSafeMagnet.NotNull[T, M]

  // TODO: the following should be obsolete
  type Nullable[T] = NullSafeMagnet.CanBeNull[T, Cap]

  object Nullable {

    type NOT[T] = NullSafeMagnet.NotNull[T, Cap]

    def NOT: NullSafeMagnet.NotNull.type = NullSafeMagnet.NotNull
  }
}
