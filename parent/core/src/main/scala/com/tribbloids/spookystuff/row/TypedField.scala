package com.tribbloids.spookystuff.row

import org.apache.spark.sql.types.{DataType, Metadata, StructField}

//used to convert SquashedRow to DF
case class TypedField(
    self: Field,
    dataType: DataType,
    metaData: Metadata = Metadata.empty
) {

  def toStructField: StructField = StructField(
    self.name,
    dataType
  )
}
