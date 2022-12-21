package org.apache.spark.ml.dsl.utils.messaging

trait MessageAPI extends Serializable {}

object MessageAPI extends Relay.ToSelf[MessageAPI] {

  trait << extends MessageAPI {

    def toProto_<< : Any
  }
}
