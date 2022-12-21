package org.apache.spark.sql.utils

import org.apache.spark.ml.dsl.utils.messaging.{MessageAPI, Relay, TreeIR}
import org.apache.spark.sql.types.DataType
import org.json4s.JValue

/**
  * Created by peng on 31/01/17.
  */
object DataTypeRelay extends Relay.<<[DataType] {

  def toJsonAST(dataType: DataType): JValue = {
    dataType.jsonValue
  }

  def fromJsonAST(jv: JValue): DataType = {
    DataType.parseDataType(jv)
  }

  override def toMessage_>>(v: DataType): IR_>> = {

    TreeIR
      .leaf(
        Msg(toJsonAST(v))
      )
  }

  case class Msg(
      dataType: JValue
  ) extends MessageAPI.<< {

    override def toProto_<< : DataType = fromJsonAST(dataType)
  }
}
