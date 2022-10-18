package org.apache.spark.ml.dsl.utils.messaging

import org.json4s.JsonAST.JString
import org.json4s.{CustomSerializer, JNull}

import java.sql.Date

object DateSerializer
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
