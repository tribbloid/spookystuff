package org.apache.spark.ml.dsl.utils.messaging

import org.json4s.JsonAST.JValue

object MetadataRelay extends MessageRelay[Map[String, Any]] {

  case class M(
                override val proto: Map[String, JValue]
              ) extends MessageAPI_<=>[Map[String, Any]] {

    override def toObject: Map[String, Any] = {
      val result: Map[String, Any] = proto.mapValues {
        jv =>
          jv.values
      }
      result
    }
  }

  override def toM(m: Map[String, Any]) = {
    val result: Map[String, JValue] = m.mapValues {
      case jv: JValue =>
        jv
      case v@ _ =>
        MessageView(v).toJValue
    }
    M(result)
  }
}
