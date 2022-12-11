package org.apache.spark.ml.dsl.utils.messaging

trait RelayLevel1 {

//  def formats: Formats = Codec.defaultFormats

  type Msg // Message type
//  implicit protected def messageMF: Manifest[Msg]

  //  implicit def proto2Message(m: M): MessageWriter[M] = {
  //    MessageWriter[M](m, this.formats)
  //  }
}
