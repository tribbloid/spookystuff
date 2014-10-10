package org.tribbloid.spookystuff.integration.scientific

import org.apache.spark.sql.SchemaRDD
import org.tribbloid.spookystuff.integration.TestCore
import org.tribbloid.spookystuff.entity.client._
import org.tribbloid.spookystuff.operator._

import scala.collection.immutable.ListMap

/**
 * Created by peng on 10/9/14.
 */
object SigmaAldrich extends TestCore {

  import spooky._

  override def doMain(): SchemaRDD = {
    val base = (noInput
      +> Wget("http://www.sigmaaldrich.com/life-science/life-science-catalog.html")
      !=!())
      .wgetJoin("table.normal tr:nth-of-type(n+2) a")(limit=2)
      .wgetJoin("li.section_square a")(joinType = Replace, limit=2)
      .wgetJoin("li.section_square a")(joinType = Replace, limit=2)
      .wgetJoin("li.section_square a")(joinType = Replace, limit=2)
      .wgetJoin("li.section_square a")(joinType = Replace, limit=2)
      .wgetJoin("li.section_square a")(joinType = Replace, limit=2)
      .wgetJoin("li.section_square a")(joinType = Replace, limit=2)
      .extract(
        "url" -> (_.resolvedUrl),
        "breadcrumb" -> (_.text1("div.crumb p")),
        "header" -> (_.text("table.opcTable thead tr th[class!=nosort]"))
      )
      .sliceJoin("table.opcTable tbody tr[class!=opcparow]")(indexKey = "row")
      .extract(
        "content" -> (_.text("td[class!=pricingButton]"))
      )
      .select(
        "KV" -> (row => ListMap(row("header").asInstanceOf[Array[String]].zip(row("content").asInstanceOf[Array[String]]): _*))
      )
      .remove("header","content")

//    base.persist()
//
//    base.asJsonRDD().collect().foreach(println)

    base
      .asSchemaRDD()
  }
}
