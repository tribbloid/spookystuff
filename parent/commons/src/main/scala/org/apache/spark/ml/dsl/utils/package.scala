package org.apache.spark.ml.dsl

import org.apache.spark.ml.dsl.utils.NullSafeMagnet.Cap

package object utils {

  type ??[T, M <: Cap] = NullSafeMagnet.CanBeNull[T, M]

  type !![T, M <: Cap] = NullSafeMagnet.NotNull[T, M]

}
