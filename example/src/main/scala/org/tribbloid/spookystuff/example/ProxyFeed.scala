package org.tribbloid.spookystuff.example

import org.tribbloid.spookystuff.actions._
import org.tribbloid.spookystuff.factory.driver.{ProxySetting, RandomProxyFactory}

import scala.concurrent.duration._

/**
 * Created by peng on 9/11/14.
 */
trait ProxyFeed extends TestCore {

  import spooky._
  import sql._

  spooky.proxy = RandomProxyFactory(proxies)

  lazy val proxies = {
    proxyRDD
      .select('IP, 'Port, "http" as 'Type)
      .map(row => ProxySetting(row.getString(0), row.getInt(1), row.getString(2)))
      .collect().toSeq
  }

  lazy val proxyRDD = {

    val httpPageRowRDD = noInput
      .fetch(
        Visit("http://www.us-proxy.org/")
          +> WaitForDocumentReady
          +> Paginate(
          "a.next:not([class*=ui-state-disabled])",
          limit =15
        ),
        indexKey = 'page
      )
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

    val socksPageRowRDD = noInput
      .fetch(
        Visit("http://www.socks-proxy.net/")
          +> WaitForDocumentReady
          +> Paginate(
          "a.next:not([class*=ui-state-disabled])",
          limit =15
        ),
        indexKey = 'page
      )
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
    //      .where(('Anonymity !== "transparent")&& 'Code.like("US"))
  }

  this.spooky.remoteResourceTimeout = 120.seconds
}
