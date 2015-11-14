package com.tribbloids.spookystuff.pipeline.google

import java.util.UUID

import com.tribbloids.spookystuff.actions._
import com.tribbloids.spookystuff.http.HttpUtils
import com.tribbloids.spookystuff.pipeline.RemoteTransformer
import com.tribbloids.spookystuff.sparkbinding.PageRowRDD
import com.tribbloids.spookystuff.{SpookyContext, dsl}

class WebSearchTransformer(
                            override val uid: String =
                            classOf[WebSearchTransformer].getCanonicalName + "_" + UUID.randomUUID().toString
                            ) extends RemoteTransformer {

  import dsl._
  import org.apache.spark.ml.param._

  /**
   * Param for input column name.
   * @group param
   */
  final val InputCol: Param[Symbol] = new Param[Symbol](this, "InputCol", "input column name")
  final val MaxPages: Param[Int] = new Param[Int](this, "MaxPages", "number of pages")
  final val PageNumCol: Param[Symbol] = new Param[Symbol](this, "PageNumCol", "output page number column name")
  final val IndexCol: Param[Symbol] = new Param[Symbol](this, "IndexCol", "output index number column name")
  final val UriCol: Param[Symbol] = new Param[Symbol](this, "UriCol", "output URI column name")

  setDefault(MaxPages -> 0, PageNumCol -> null, IndexCol -> null, UriCol -> null)
  setExample(InputCol -> '_, MaxPages -> 2, PageNumCol -> 'page, IndexCol -> 'index, UriCol -> 'uri)

  override def exampleInput(spooky: SpookyContext): PageRowRDD = spooky.create(Seq("Giant Robot"))

  override def transform(dataset: PageRowRDD): PageRowRDD = {

    dataset.fetch(
      Visit("http://www.google.com/") +>
        TextInput("input[name=\"q\"]",getOrDefault(InputCol)) +>
        Submit("input[name=\"btnG\"]")
    )
      .wgetExplore(S"div#foot a:contains(Next)", maxDepth = getOrDefault(MaxPages), depthKey = getOrDefault(PageNumCol), optimizer = Narrow)
      .wgetJoin(S".g h3.r a".hrefs.flatMap {
        uri =>
          val query = HttpUtils.uri(uri).getQuery
          val realURI = if (query == null) Some(uri)
          else if (uri.contains("/url?")) query.split("&").find(_.startsWith("q=")).map(_.replaceAll("q=",""))
          else None
          realURI
      },
        ordinalKey = getOrDefault(IndexCol),
        failSafe = 2, //not all links are viable
        select = 'A ~ getOrDefault(UriCol)
      )
  }
}