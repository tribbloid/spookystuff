package com.tribbloids.spookystuff.pipeline.transformer.google

import java.util.UUID

import com.tribbloids.spookystuff.actions._
import com.tribbloids.spookystuff.http.HttpUtils
import com.tribbloids.spookystuff.pipeline.SpookyTransformer
import com.tribbloids.spookystuff.sparkbinding.PageRowRDD
import com.tribbloids.spookystuff.{SpookyContext, dsl}

class WebSearchTransformer(
                            override val uid: String =
                            classOf[WebSearchTransformer].getCanonicalName + "_" + UUID.randomUUID().toString
                            ) extends SpookyTransformer {

  import dsl._
  import org.apache.spark.ml.param._

  /**
   * Param for input column name.
   * @group param
   */
  final val InputCol: Param[Symbol] = new Param[Symbol](this, "inputCol", "input column name")
  final val MaxPages: Param[Int] = new Param[Int](this, "MaxPages", "number of pages")
  final val PageNumCol: Param[Symbol] = new Param[Symbol](this, "PageNumCol", "output page number column name")
  final val IndexCol: Param[Symbol] = new Param[Symbol](this, "IndexCol", "output index number column name")

  setDefault(MaxPages -> 0, PageNumCol -> null, IndexCol -> null)

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
        failSafe = 2 //not all links are viable
      )
  }

  def test(spooky: SpookyContext): Unit = {

    import org.apache.spark.sql.functions._
    import spooky.sqlContext.implicits._

    val source = spooky.create(Seq(Map("input" -> "Giant Robot")))

    val transformer = new WebSearchTransformer()
      .setInputCol('input)
      .setMaxPages(2)
      .setPageNumCol('page)
      .setIndexCol('index)

    val result = transformer.transform(source)
//      .select(
//        S.uri ~ 'uri,
//        (S \\ "title").text ~ 'title
//      )

    val df = result.toDF(sort = true).persist()

    df.collect().foreach(println)
    val maxPage = df.agg(max($"page"))
    assert(maxPage.collect().head.getLong(0) == 2)

    val aggIndex = df
      .select(explode('index) as 'index0, 'page)
      .groupBy('page).agg(max('index0), count('index0))
    aggIndex.collect().foreach {
      row =>
        assert(row(1).asInstanceOf[Long] >=5 )
        assert(row(1) == row(2).asInstanceOf[Long] - 1)
    }
  }
}