package org.apache.spark.ml.dsl.utils

import org.json4s._

import scala.concurrent.duration.Duration

/**
  * Different from json4s.ext.DurationSerializer in that it doesn't serialize to long value
  */
object DurationJSONSerializer extends CustomSerializer[Duration](
  format =>
    (
      {
        case JString(d) => Duration(d)
        case JNull => null
      },
      {
        case d: Duration =>
          if (d.isFinite()) JString(d.toString)
          else JString(d.toString.split("\\.").last)
      }
      )
)