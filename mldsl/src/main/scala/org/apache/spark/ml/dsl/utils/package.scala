package org.apache.spark.ml.dsl

package object utils {

  type Nullable[T] = NullSafe.Immutable[T]

  val Nullable: NullSafe.type = NullSafe
}
