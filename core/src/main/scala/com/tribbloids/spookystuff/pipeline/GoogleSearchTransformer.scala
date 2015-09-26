package com.tribbloids.spookystuff.pipeline

import com.tribbloids.spookystuff.actions._
import com.tribbloids.spookystuff.dsl
import com.tribbloids.spookystuff.http.HttpUtils
import com.tribbloids.spookystuff.sparkbinding.PageRowRDD
import org.apache.spark.ml.util.Identifiable


class GoogleSearchTransformer(
                               override val uid: String = Identifiable.randomUID("tok")
                               ) extends DynamicSetter {

  import dsl._
  import org.apache.spark.ml.param._

  /**
   * Param for input column name.
   * @group param
   */
  final val InputCol: Param[String] = new Param[String](this, "inputCol", "input column name")
  final val Pages: Param[Int] = new Param[Int](this, "Pages", "number of pages")
  final val PageCol: Param[String] = new Param[String](this, "PageCol", "output page number column name")
  final val IndexCol: Param[String] = new Param[String](this, "IndexCol", "output index number column name")

  setDefault(Pages -> 0, PageCol -> null, IndexCol -> null)

  override def transform(dataset: PageRowRDD): PageRowRDD = {

    dataset.fetch(
      Visit("http://www.google.com/") +>
        TextInput("input[name=\"q\"]",$(InputCol)) +>
        Submit("input[name=\"btnG\"]")
    )
      .wgetExplore(S"div#foot a:contains(Next)", maxDepth = getOrDefault(Pages), depthKey = $(PageCol), optimizer = Narrow)
      .wgetJoin(S".g h3.r a".hrefs.flatMap {
      uri =>
        val query = HttpUtils.uri(uri).getQuery
        val realURI = if (query == null) Some(uri)
        else if (uri.contains("/url?")) query.split("&").find(_.startsWith("q=")).map(_.replaceAll("q=",""))
        else None
        realURI
    },
        ordinalKey = $(IndexCol),
        failSafe = 2 //not all links are viable
      )
  }
}
