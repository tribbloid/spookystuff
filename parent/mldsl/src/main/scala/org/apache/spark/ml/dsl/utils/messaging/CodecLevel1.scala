package org.apache.spark.ml.dsl.utils.messaging

import org.json4s.Formats


trait CodecLevel1 {

  def formats: Formats = Codec.defaultFormats

  type M // Message type
  implicit protected def messageMF: Manifest[M]

  //  implicit def proto2Message(m: M): MessageWriter[M] = {
  //    MessageWriter[M](m, this.formats)
  //  }
}
