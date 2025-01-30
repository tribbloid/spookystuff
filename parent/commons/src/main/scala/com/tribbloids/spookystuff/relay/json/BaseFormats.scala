package com.tribbloids.spookystuff.relay.json

import com.tribbloids.spookystuff.commons.lifespan.ThreadLocal
import org.json4s.{DateFormat, DefaultFormats}

import java.text.SimpleDateFormat
import java.util.{Date, TimeZone}
import scala.util.Try

object BaseFormats extends DefaultFormats {

  override val dateFormat: DateFormat = new DateFormat {

    def parse(s: String): Some[Date] =
      dateFormats.flatMap { format =>
        Try {
          Some(format.parse(s))
        }.toOption
      }.head

    def format(d: Date): String = dateFormats.head.format(d)

    def timezone: TimeZone = dateFormats.head.getTimeZone

    val dateFormatFactory: ThreadLocal[Seq[SimpleDateFormat]] = ThreadLocal { _ =>
      Seq(
        new java.text.SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS"),
        new java.text.SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss"),
        new java.text.SimpleDateFormat("yyyy-MM-dd'T'HH:mm")
      )
    }

    def dateFormats: Seq[SimpleDateFormat] = dateFormatFactory.get()
  }

  override val wantsBigDecimal: Boolean = true
}
