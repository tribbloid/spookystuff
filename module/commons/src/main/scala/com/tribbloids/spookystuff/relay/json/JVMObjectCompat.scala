package com.tribbloids.spookystuff.relay.json

import com.fasterxml.jackson.databind.ObjectMapper
import org.json4s.jackson.JsonMethods.{compact, parse, render}

object JVMObjectCompat {

  import org.json4s.*

  object JacksonSerDe extends Serializer[AnyRef] {
    private val jacksonMapper = new ObjectMapper()

    def deserialize(
        implicit
        format: Formats
    ): PartialFunction[(TypeInfo, JValue), AnyRef] = {
      case (TypeInfo(clazz, _), json) if classOf[AnyRef].isAssignableFrom(clazz) =>
        val result = jacksonMapper.readValue(compact(render(json)), clazz)
        result match {
          case v: AnyRef => v
//          case v         => throw new MatchError(v)
        }
    }

    def serialize(
        implicit
        format: Formats
    ): PartialFunction[Any, JValue] = {
      case obj if obj.getClass.getName.startsWith("java.") =>
        parse(jacksonMapper.writeValueAsString(obj))
    }
  }

}
