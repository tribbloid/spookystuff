package com.tribbloids.spookystuff.extractors

import com.tribbloids.spookystuff.extractors.GenExtractor.{HasAlias, Static}
import com.tribbloids.spookystuff.row.{Alias, Field};

case class GenExpr[T, +R](
    override val resolved: PartialFunction[T, R],
    field: Field
) extends Static[T, R]
    with HasAlias[T, R] {

  override def alias: Alias = field.alias

  override val dataType: DataType = field.dataType
}
