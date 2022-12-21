package org.apache.spark.ml.dsl.utils.messaging

trait ProtoAPI {

  def toMessage_>> : IR
}

object ProtoAPI extends Relay[ProtoAPI] {

  override type IR_>> = IR
  override def toMessage_>>(v: ProtoAPI) = v.toMessage_>>

  override def toProto_<<(v: IR_<<): ProtoAPI = ???
}
