package com.tribbloids.spookystuff.extractors

import com.tribbloids.spookystuff.extractors.GenExtractor.Static
import com.tribbloids.spookystuff.row.{Field, TypedField};

case class GenResolved[T, +R](
    partialFunction: PartialFunction[T, R],
    typedField: TypedField
) extends Static[T, R]
    with Alias[T, R] {

  def field: Field = typedField.self

  override val dataType: DataType = typedField.dataType
}
