package com.tribbloids.spookystuff.relay.json

import org.json4s.JsonAST.JString
import org.json4s.{CustomSerializer, JNull}

import java.sql.Date

object SQLDateCompat {

  object _Serializer
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

}
