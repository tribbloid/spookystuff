package com.tribbloids.spookystuff.extractors

import com.tribbloids.spookystuff.extractors.GenExtractor.Static
import com.tribbloids.spookystuff.row.TypedField
;

case class GenResolved[T, +R](
                               self: PartialFunction[T, R],
                               typedField: TypedField
                             ) extends Static[T, R] with Alias[T, R]{

  def field = typedField.self

  override val dataType: DataType = typedField.dataType
}