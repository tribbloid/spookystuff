package org.apache.spark.ml.dsl

package object utils {

  type Var = NullSafe.Var

  type ?[T, M] = NullSafe.CanBeNull[T, M]

  type ![T, M] = NullSafe.CannotBeNull[T, M]

  // TODO: the following should be obsolete
  type Nullable[T] = NullSafe.CanBeNull[T, Any]

  object Nullable {

    type NOT[T] = NullSafe.CannotBeNull[T, Any]

    def NOT: NullSafe.CannotBeNull.type = NullSafe.CannotBeNull
  }
}
