package com.tribbloids.spookystuff.relay.xml

import com.tribbloids.spookystuff.utils.ThreadLocal
import org.json4s.{DateFormat, DefaultFormats, Formats}

import java.text.SimpleDateFormat
import java.util.{Date, TimeZone}
import scala.util.Try
import scala.xml.PrettyPrinter

object XMLFormats {

  val baseDataFormatsFactory: ThreadLocal[Seq[SimpleDateFormat]] = ThreadLocal { _ =>
    Seq(
      new java.text.SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS"),
      new java.text.SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss"),
      new java.text.SimpleDateFormat("yyyy-MM-dd'T'HH:mm")
    )
  }

  object BaseFormat extends DefaultFormats {

    override val dateFormat: DateFormat = new DateFormat {
      def parse(s: String): Some[Date] =
        dateFormats.flatMap { format =>
          Try {
            Some(format.parse(s))
          }.toOption
        }.head

      def format(d: Date): String = dateFormats.head.format(d)

      def timezone: TimeZone = dateFormats.head.getTimeZone

      def dateFormats: Seq[SimpleDateFormat] = baseDataFormatsFactory.get()
    }

    override val wantsBigDecimal: Boolean = true
  }

  def xmlFormats(base: Formats = BaseFormat): Formats =
    base +
      StringToNumberDeserializer +
      EmptyStringToEmptyObjectDeserializer +
      ElementToArrayDeserializer
  //      DurationJSONSerializer
  //  +
  //    FallbackJSONSerializer

  lazy val defaultFormats: Formats = xmlFormats()

  val defaultXMLPrinter: PrettyPrinter = new scala.xml.PrettyPrinter(80, 2)
}
