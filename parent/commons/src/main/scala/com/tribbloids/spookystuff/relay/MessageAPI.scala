package com.tribbloids.spookystuff.relay

trait MessageAPI extends Serializable {}

object MessageAPI extends Relay.ToSelf[MessageAPI] {

  trait << extends MessageAPI {

    def toProto_<< : Any
  }
}
