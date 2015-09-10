package com.tribbloids.spookystuff.example.ml

import org.apache.spark.mllib.clustering.LDA
import org.apache.spark.mllib.feature.{HashingTF, IDF}
import com.tribbloids.spookystuff.SpookyContext
import com.tribbloids.spookystuff.dsl._
import com.tribbloids.spookystuff.example.{Common, QueryCore}
import com.tribbloids.spookystuff.http.HttpUtils

/**
 * Created by peng on 16/06/15.
 */

object Google_LDA extends QueryCore {

  //  case class EntityKeywords(entity: String, associated: String, frequency: String)

  override def doMain(spooky: SpookyContext) = {
    import spooky.dsl._
    import sql.implicits._

    val raw = sc.parallelize(Seq("parrot")).fetch(
      Common.googleSearch('_)
    ).wgetExplore(S"a:contains(next)", maxDepth = 2, depthKey = 'page, optimizer = Narrow
      ).wgetJoin(S"li.g h3.r a".hrefs.flatMap {
      uri =>
        val query = HttpUtils.uri(uri).getQuery
        val realURI = if (query == null) Some(uri)
        else if (uri.contains("/url?")) query.split("&").find(_.startsWith("q=")).map(_.replaceAll("q=",""))
        else None
        realURI
    }
        ,failSafe = 2 ).select(
        S.boilerPipe.orElse(Some("")) ~ 'text
      ).toDF()

    val tokenized =
    //      new RegexTokenizer().setInputCol("text").setOutputCol("words").setMinTokenLength(4).setPattern("[^A-Za-z]").transform(raw)
      raw
        .select('words).map(row => row.getAs[Seq[String]](0))
    val stemmed = tokenized.map(_.map(v => v.toLowerCase)).persist()

    val hashingTF = new HashingTF(1 << 22)
    val tf = hashingTF.transform(stemmed).persist()
    val idf = new IDF().fit(tf)
    val tfidf = idf.transform(tf)

    val kvs = stemmed.map {
      _.map {
        word =>
          hashingTF.indexOf(word) -> word
      }
    }.reduce(_ ++ _)
    //    val numKeys = kvs.map(_._1).distinct.size
    //    val numPairs = kvs.distinct.size
    //    assert(numKeys == numPairs, s"$numKeys : $numPairs")
    val lookup = sc.broadcast(Map(kvs: _*))

    val documents = tfidf.zipWithIndex().map(tuple => tuple._2 -> tuple._1)

    // Set LDA parameters
    val numTopics = 8
    val lda = new LDA().setK(numTopics).setMaxIterations(20)

    val ldaModel = lda.run(documents)

    // Print topics, showing top-weighted 10 terms for each topic.
    sc.parallelize(ldaModel.describeTopics(10))
      .flatMap {
      v =>
        val words = v._1.toSeq.map(lookup.value.get(_).get)
        val groupTag = words.sorted.mkString(" ")
        words.zip(v._2.toSeq).map(tuple => EntityKeyword(groupTag, tuple._1, tuple._2.toString))
    }.toDF()
      .fetch(
        Common.googleSearch('entity)
      )
      .select(x"%html ${S"li.g h3.r a".filter(!_.href.getOrElse("").contains("/image?")).code}" ~ 'topic)
      .toDF().select('topic, 'word, 'frequency, 'entity)
  }
}