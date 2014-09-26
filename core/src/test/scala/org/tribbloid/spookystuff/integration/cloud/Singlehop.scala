package org.tribbloid.spookystuff.integration.cloud

import org.tribbloid.spookystuff.entity.client.Wget
import org.tribbloid.spookystuff.integration.SpookyTestCore

/**
* Created by peng on 25/08/14.
*/
object Singlehop extends SpookyTestCore {

  import spooky._

  override def doMain() = {
    (noInput
      +> Wget("http://www.singlehop.com/server-hosting/dedicated-servers/")
      !=!())
      .sliceJoin("tbody tr")()
      .extract(
        "CPU" -> {_.text1("span")},
        "Core/Freq" -> {_.text("td")(2)},
        "Memory" -> {_.text("td")(3)},
        "Drive" -> {_.text("td")(4)},
        "Bandwidth" -> {_.text("td")(5)},
        "Availability" -> {_.text("td")(6)},
        "price" -> {_.text1("b")}
      ).asSchemaRDD()
  }
}