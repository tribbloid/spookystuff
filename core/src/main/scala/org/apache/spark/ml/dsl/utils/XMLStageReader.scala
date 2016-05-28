package org.apache.spark.ml.dsl.utils

import org.apache.spark.ml.param.Params
import org.apache.spark.ml.util.DefaultParamsReader.Metadata
import org.apache.spark.ml.util.{DefaultParamsReader, MLReadable, MLReader}
import org.apache.spark.util.Utils
import org.json4s.JsonAST.JObject
import org.json4s.jackson.JsonMethods._

/**
  * Created by peng on 16/05/16.
  */
class XMLStageReader[T <: Params](implicit val mf: Manifest[T]) extends DefaultParamsReader[T] with XMLReaderMixin[T] {

  override def load(path: String): T = {
    val metadata = DefaultParamsReader.loadMetadata(path, sc)
    val cls = Utils.classForName(metadata.className)
    val instance =
      cls.getConstructor(classOf[String]).newInstance(metadata.uid).asInstanceOf[T]
    this.getAndSetParams(instance, metadata)
    instance
  }

  /**
    * Extract Params from metadata, and set them in the instance.
    * This works if all Params implement [[org.apache.spark.ml.param.Param.jsonDecode()]].
    */
  def getAndSetParams(instance: T, metadata: Metadata): Unit = {
    metadata.params match {
      case JObject(pairs) =>
        pairs.foreach { case (paramName, jValue) =>
          val param = instance.getParam(paramName)
          val value = param.jsonDecode(compact(render(jValue)))
          instance.set(param, value)
        }
      case _ =>
        throw new IllegalArgumentException(
          s"Cannot recognize JSON metadata: ${metadata.metadataJson}.")
    }
  }
}

class XMLStageReadable[T <: Params](implicit val mf: Manifest[T]) extends MLReadable[T] {

  object Reader extends XMLStageReader[T]

  @org.apache.spark.annotation.Since("1.6.0")
  override def read: MLReader[T] = Reader
}