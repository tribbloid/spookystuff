package org.tribbloid.spookystuff.integration

import org.tribbloid.spookystuff.entity.clientaction.{DelayForDocumentReady, LoadMore, Visit}

/**
 * Created by peng on 9/11/14.
 */
trait ProxyFeed extends SpookyTestCore {

  import spooky._
  import sql._

  lazy val proxies = {
    proxyRDD.select('IP, 'Port, "http" as 'Type)
      .collect().map(row => (row.getString(0)+":"+row.getString(1), row.getString(2)))
  }

  lazy val proxyRDD = {

    val httpPageRowRDD = (noInput
      +> Visit("http://www.us-proxy.org/")
      +> DelayForDocumentReady()
      +> LoadMore(
      "a.next",
      limit =15,
      snapshot = true,
      mustExist = "a.next:not([class*=ui-state-disabled])"
    )
      !=!(indexKey = "page"))
      .sliceJoin("table.dataTable tbody tr")()
      .extract(
        "IP" -> (_.text("td")(0)),
        "Port" -> (_.text("td")(1)),
        "Code" -> (_.text("td")(2)),
        "Country" -> (_.text("td")(3)),
        "Anonymity" -> (_.text("td")(4)),
        "Google" -> (_.text("td")(5)),
        "Https" -> (_.text("td")(6)),
        "LastChecked" -> (_.text("td")(7)),
        "Type" -> (page => "http")
      )


    val socksPageRowRDD = (noInput
      +> Visit("http://www.socks-proxy.net/")
      +> DelayForDocumentReady()
      +> LoadMore(
      "a.next",
      limit =15,
      snapshot = true,
      mustExist = "a.next:not([class*=ui-state-disabled])"
    )
      !=!(indexKey = "page"))
      .sliceJoin("table.dataTable tbody tr")()
      .extract(
        "IP" -> (_.text("td")(0)),
        "Port" -> (_.text("td")(1)),
        "Code" -> (_.text("td")(2)),
        "Country" -> (_.text("td")(3)),
        "Version" -> (_.text("td")(4)),
        "Anonymity" -> (_.text("td")(5)),
        "Https" -> (_.text("td")(6)),
        "LastChecked" -> (_.text("td")(7)),
        "Type" -> (page => "socks5")
      )

    httpPageRowRDD.union(socksPageRowRDD)
      .asSchemaRDD()
      .where(('Anonymity !== "transparent")&&('Code.like("US")))
  }
}
