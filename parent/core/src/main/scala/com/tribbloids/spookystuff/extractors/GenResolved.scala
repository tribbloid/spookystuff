package com.tribbloids.spookystuff.extractors

import com.tribbloids.spookystuff.extractors.GenExtractor.Static
import com.tribbloids.spookystuff.row.Field.TypedField
import com.tribbloids.spookystuff.row.Field

case class GenResolved[T, +R](
    override val resolved: PartialFunction[T, R],
    typedField: TypedField
) extends Static[T, R]
    with Alias[T, R] {

  def field: Field = typedField.self

  override val dataType: DataType = typedField.dataType
}
