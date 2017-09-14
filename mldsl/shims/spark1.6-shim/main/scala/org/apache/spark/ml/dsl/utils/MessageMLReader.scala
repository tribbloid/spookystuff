package org.apache.spark.ml.dsl.utils

import org.apache.spark.ml.dsl.utils.messaging.MessageRelay
import org.apache.spark.ml.util.MLReader

/**
  * Created by peng on 05/10/16.
  */
case class MessageMLReader[Obj](outer: MessageRelay[Obj]) extends MLReader[Obj] {

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
