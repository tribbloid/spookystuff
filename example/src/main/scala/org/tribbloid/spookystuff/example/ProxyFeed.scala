package org.tribbloid.spookystuff.example

import org.apache.spark.sql.SchemaRDD
import org.tribbloid.spookystuff.SpookyContext
import org.tribbloid.spookystuff.actions._
import org.tribbloid.spookystuff.factory.driver.{ProxySetting, RandomProxyFactory}

/**
 * Created by peng on 9/11/14.
 */
trait ProxyFeed extends ExampleCore {

  override def localPreviewContext(): SpookyContext = {
    val spooky = super.localPreviewContext()

//    val proxies = proxyRDD(spooky).select('IP, 'Port, 'Type)
//      .map(row => ProxySetting(row.getString(0), row.getInt(1), row.getString(2)))
//      .collect().toSeq

//    spooky.proxy = RandomProxyFactory(proxies)
    spooky
  }

  override def fullRunContext(): SpookyContext = {
    val spooky = super.fullRunContext()

    import sql._

    val proxies = proxyRDD(spooky).select('IP, 'Port, 'Type)
      .map(row => ProxySetting(row.getString(0), row.getInt(1), row.getString(2)))
      .collect().toSeq

    spooky.proxy = RandomProxyFactory(proxies)
    spooky
  }

  def proxyRDD(spooky: SpookyContext): SchemaRDD = {

    import spooky._

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
      ).persist()

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
        "Type" -> (_ => "socks5")
      ).persist()

    httpPageRowRDD.union(socksPageRowRDD)
      .asSchemaRDD()
    //      .where(('Anonymity !== "transparent")&& 'Code.like("US"))
  }
}