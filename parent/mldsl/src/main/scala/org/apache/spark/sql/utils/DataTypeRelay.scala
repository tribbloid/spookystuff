package org.apache.spark.sql.utils

import org.apache.spark.ml.dsl.utils.messaging.{MessageAPI_<<, MessageRelay}
import org.apache.spark.sql.types.DataType
import org.json4s.JValue

/**
  * Created by peng on 31/01/17.
  */
object DataTypeRelay extends MessageRelay[DataType] {

  def toJsonAST(dataType: DataType): JValue = {
    dataType.jsonValue
  }

  def fromJsonAST(jv: JValue): DataType = {
    DataType.parseDataType(jv)
  }

  override def toMessage_>>(v: DataType): M = M(
    toJsonAST(v)
  )

  case class M(
      dataType: JValue
  ) extends MessageAPI_<< {

    override def toProto_<< : DataType = fromJsonAST(dataType)
  }
}
