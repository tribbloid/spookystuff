package org.apache.spark.ml.dsl.utils.messaging
import org.apache.spark.ml.dsl.utils.messaging

trait ProtoAPI extends RootTagged {

  def toMessage_>> : Any
}

object ProtoAPI extends Relay[ProtoAPI] {

  override type Msg = Any
  override def toMessage_>>(v: ProtoAPI) = v.toMessage_>>

  override def toProto_<<(v: messaging.ProtoAPI.Msg, rootTag: String): ProtoAPI = ???
}

trait MessageAPI extends RootTagged with Serializable {}

object MessageAPI extends MessageReader[MessageAPI] {

  trait << extends MessageAPI {

    def toProto_<< : Any
  }
}
