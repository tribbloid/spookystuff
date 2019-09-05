package org.apache.spark.ml.dsl.utils

import java.text.SimpleDateFormat
import java.util.{Date, TimeZone}

import com.tribbloids.spookystuff.utils.ThreadLocal
import org.json4s._

import scala.util.Try

object XMLFormats extends DefaultFormats {

  val baseDataFormatsFactory: ThreadLocal[Seq[SimpleDateFormat]] = ThreadLocal { _ =>
    Seq(
      new java.text.SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS"),
      new java.text.SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss"),
      new java.text.SimpleDateFormat("yyyy-MM-dd'T'HH:mm")
    )
  }

  val baseFormat: XMLFormats.type = XMLFormats

  def xmlFormats(base: Formats = baseFormat): Formats =
    base +
      StringToNumberDeserializer +
      EmptyStringToEmptyObjectDeserializer +
      ElementToArrayDeserializer
  //      DurationJSONSerializer
  //  +
  //    FallbackJSONSerializer

  lazy val defaultFormats: Formats = xmlFormats()

  val defaultXMLPrinter = new scala.xml.PrettyPrinter(80, 2)
  override val dateFormat: DateFormat = new DateFormat {
    def parse(s: String): Some[Date] =
      dateFormats.flatMap { format =>
        Try {
          Some(format.parse(s))
        }.toOption
      }.head

    def format(d: Date): String = dateFormats.head.format(d)

    def timezone: TimeZone = dateFormats.head.getTimeZone

    def dateFormats: Seq[SimpleDateFormat] = baseDataFormatsFactory()
  }

  override val wantsBigDecimal = true
}
