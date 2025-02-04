package com.tribbloids.spookystuff.relay.json

import org.json4s.{CustomSerializer, JNull, JString}

import java.sql.Date
import scala.concurrent.duration.Duration

object SpecificTypeCompat {

  object SQLDateSerDe
      extends CustomSerializer[Date](_ =>
        (
          {
            case JString(s) => Date.valueOf(s)
            case JNull      => null
          },
          {
            case d: Date => JString(d.toString)
          }
        )
      )

  /**
    * Different from json4s.ext.DurationSerializer in that it doesn't serialize to long value
    */
  object DurationJSONSerDe
      extends CustomSerializer[Duration](_ =>
        (
          {
            case JString(d) => Duration(d)
            case JNull      => null
          },
          {
            case d: Duration =>
              if (d.isFinite) JString(d.toString)
              else JString(d.toString.split("\\.").last)
          }
        )
      )

}
