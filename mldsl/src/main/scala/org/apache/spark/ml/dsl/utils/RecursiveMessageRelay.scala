package org.apache.spark.ml.dsl.utils

import com.tribbloids.spookystuff.utils.ReflectionUtils

/**
  * Created by peng on 31/05/17.
  */
object RecursiveMessageRelay extends MessageRelay[Any] {

  //  override implicit def formats: Formats = Xml.defaultFormats + FallbackJSONSerializer

  //avoid using scala reflections on worker as they are thread unsafe, use JSON4s that is more battle tested
  def transform(value: Any)(f: PartialFunction[Any, Any]): Any = {
    val result = f.applyOrElse[Any, Any](
      value,
      {
        case v: Map[_,_] =>
          v.mapValues(v => transform(v)(f))
        case v: Traversable[_] =>
          v.map(v => transform(v)(f))
        case (k: String, v: Any) =>
          k -> transform(v)(f)
        case v: Product =>
          try {
            val map = Map(ReflectionUtils.getCaseAccessorMap(v): _*)
            map.mapValues(v => transform(v)(f))
          }
          catch {
            case _: Throwable => v
          }
        case v =>
          v
      }
    )
    result
  }

  //TODO: change to MessageRepr to allow 2-way conversions.
  case class M(
                override val toMessage: Any
              ) extends MessageAPI {

    override def formats = RecursiveMessageRelay.this.formats
  }

  override def toMessage(v: Any): M = {
    val msg = transform(v){
      case v: MessageAPI =>
        v.toMessage
    }
    M(msg)
  }
}
