package org.apache.spark.ml.dsl

import org.apache.spark.ml.dsl.utils.MessageRelay
import org.eclipse.jetty.websocket.common.message.MessageReader

/**
  * Created by peng on 05/10/16.
  */
class MessageReader[Obj](outer: MessageRelay[Obj]) {

  case object MLReader extends MLReader[Obj] {

    def outer = MessageReader.outer

    //TODO: need impl
    override def load(path: String): Obj = {
      //      val metadata = DefaultParamsReader.loadMetadata(path, sc)
      //      val cls = Utils.classForName(metadata.className)
      //      val instance =
      //        cls.getConstructor(classOf[String]).newInstance(metadata.uid).asInstanceOf[Params]
      //      DefaultParamsReader.getAndSetParams(instance, metadata)
      //      instance.asInstanceOf[T]
      ???
    }
  }
}
