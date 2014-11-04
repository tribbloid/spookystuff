package org.tribbloid.spookystuff.integration.scientific

import org.apache.spark.sql.SchemaRDD
import org.tribbloid.spookystuff.entity.client._
import org.tribbloid.spookystuff.integration.TestCore
import org.tribbloid.spookystuff.operator._
import org.tribbloid.spookystuff.utils.Utils

import scala.collection.immutable.ListMap

/**
 * Created by peng on 10/9/14.
 */
object SigmaAldrich extends TestCore {

  import spooky._

  override def doMain(): SchemaRDD = {
    val base0 = (noInput
      +> Wget("http://www.sigmaaldrich.com/life-science/life-science-catalog.html")
      !=!())
      .wgetJoin("table.normal tr:nth-of-type(n+2) a")().persist()

    println(base0.count())

    val base1 = base0
      .wgetJoin("li.section_square a")(joinType = Inner).persist()

    println(base1.count())

    val base2 = base1
      .wgetJoin("li.section_square a")(joinType = Inner).persist()

    println(base2.count())

    val base3 = base2
      .wgetJoin("li.section_square a")(joinType = Inner).persist()

    println(base3.count())

    val base4 = base3
      .wgetJoin("li.section_square a")(joinType = Inner).persist()

    println(base4.count())

    val base5 = base4
      .wgetJoin("li.section_square a")(joinType = Inner).persist()

    println(base5.count())

    val base6 = base5
      .wgetJoin("li.section_square a")(joinType = Inner).persist()

    println(base6.count())

    val base7 = base6
      .wgetJoin("li.section_square a")(joinType = Inner).persist()

    println(base7.count())

    val all = base0.union(base1).union(base2).union(base3).union(base4).union(base5).union(base6).union(base7)
    import org.apache.spark.SparkContext._
    val selfRed = all.self.keyBy(_.pages.lastOption.getOrElse("")).reduceByKey((v1,v2) => v1).map(_._2)
    val allRed = all.copy(self = selfRed)

    val result = all
      .extract(
        "url" -> (_.resolvedUrl),
        "breadcrumb" -> (_.text1("div.crumb p")),
        "header" -> (_.text("table.opcTable thead tr th[class!=nosort]"))
      )
      .sliceJoin("table.opcTable tbody tr[class!=opcparow]")(joinType = Inner, indexKey = "row")
      .extract(
        "content" -> (_.text("td[class!=pricingButton]"))
      )
      .select(
        "KV" -> (row => Utils.toJson(ListMap(row("header").asInstanceOf[Array[String]].zip(row("content").asInstanceOf[Array[String]]): _*)))
      )
      .remove("header","content")

//    result.asSchemaRDD()

    val json = result.asMapRDD()

//      .map(map => (map("url") -> map("breadcrumb"), map("KV"))).groupByKey()
//      .map(
//        tuple => Map("url"->tuple._1._1, "breadcrumb"->tuple._1._2, "KVs"->tuple._2)
//      )
      .map(
        map => Utils.toJson(map)
      )

    sql.jsonRDD(json)

    //    base.asSchemaRDD().groupBy('url, 'breadcrumb)('url, 'breadcrumb, CollectHashSet('KV:: Nil) as 'KVs)
  }
}
