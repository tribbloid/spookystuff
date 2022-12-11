package org.apache.spark.ml.dsl

package object utils {

  type Var = NullSafety.Var

  type `?`[T, M] = NullSafety.CanBeNull[T, M]

  type ![T, M] = NullSafety.CannotBeNull[T, M]

  // TODO: the following should be obsolete
  type Nullable[T] = NullSafety.CanBeNull[T, Any]

  object Nullable {

    type NOT[T] = NullSafety.CannotBeNull[T, Any]

    def NOT: NullSafety.CannotBeNull.type = NullSafety.CannotBeNull
  }
}
