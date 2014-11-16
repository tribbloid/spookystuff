package org.tribbloid.spookystuff.example.scientific

import org.apache.spark.sql.SchemaRDD
import org.tribbloid.spookystuff.actions._
import org.tribbloid.spookystuff.expressions._
import org.tribbloid.spookystuff.example.TestCore
import org.tribbloid.spookystuff.utils.Utils

/**
 * Created by peng on 10/9/14.
 */
object SigmaAldrich extends TestCore {

  import spooky._

  override def doMain(): SchemaRDD = {
    val base0 = noInput
      .fetch(
        Wget("http://www.sigmaaldrich.com/life-science/life-science-catalog.html")
      )
      .wgetJoin('* href "table.normal tr:nth-of-type(n+2) a")().persist()

    println(base0.count())

    val base1 = base0
      .wgetJoin('* href "li.section_square a")(joinType = Inner).persist()

    println(base1.count())

    val base2 = base1
      .wgetJoin('* href "li.section_square a")(joinType = Inner).persist()

    println(base2.count())

    val base3 = base2
      .wgetJoin('* href "li.section_square a")(joinType = Inner).persist()

    println(base3.count())

    val base4 = base3
      .wgetJoin('* href "li.section_square a")(joinType = Inner).persist()

    println(base4.count())

    val base5 = base4
      .wgetJoin('* href "li.section_square a")(joinType = Inner).persist()

    println(base5.count())

    val base6 = base5
      .wgetJoin('* href "li.section_square a")(joinType = Inner).persist()

    println(base6.count())

    val base7 = base6
      .wgetJoin('* href "li.section_square a")(joinType = Inner).persist()

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
      .sliceJoin("table.opcTable tbody tr[class!=opcparow]")(joinType = Inner, indexKey = 'row)
      .extract(
        "content" -> (_.text("td[class!=pricingButton]"))
      )
//      .select(
//        "KV" -> (row => Utils.toJson(ListMap(row("header").asInstanceOf[Array[String]].zip(row("content").asInstanceOf[Array[String]]): _*)))
//      )
//      .remove('header,'content)

    val json = result.asMapRDD()
      .map(
        map => Utils.toJson(map)
      )

    sql.jsonRDD(json)
  }
}
