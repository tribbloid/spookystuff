package org.tribbloid.spookystuff.example.ml

import cc.factorie.app.strings.PorterStemmer
import org.apache.spark.ml.feature.RegexTokenizer
import org.apache.spark.mllib.clustering.LDA
import org.apache.spark.mllib.feature.{HashingTF, IDF}
import org.tribbloid.spookystuff.SpookyContext
import org.tribbloid.spookystuff.actions._
import org.tribbloid.spookystuff.dsl._
import org.tribbloid.spookystuff.example.QueryCore
import org.tribbloid.spookystuff.http.HttpUtils

/**
 * Created by peng on 16/06/15.
 */

object Google_LDA extends QueryCore {

//  case class EntityKeywords(entity: String, associated: String, frequency: String)

  override def doMain(spooky: SpookyContext) = {
    import spooky.dsl._
    import sql.implicits._

    val raw = sc.parallelize("parrot".split(",").map(_.trim)).fetch(
      Visit("http://www.google.com/")
        +> TextInput("input[name=\"q\"]","'{_}")
        +> Submit("input[name=\"btnG\"]")
    ).wgetExplore(S"a:contains(next)", maxDepth = 2, depthKey = 'page, optimizer = Narrow
      ).wgetJoin(S"li.g h3.r a[href^=/url]".hrefs.flatMap {
      uri =>
        HttpUtils.uri(uri).getQuery.split("&").find(_.startsWith("q=")).map(_.replaceAll("q=",""))
    },failSafe = 2 ).select(
        '$.boilerPipe.orElse(Some("")) ~ 'text
      ).toDF()

    val tokenized = new RegexTokenizer().setInputCol("text").setOutputCol("words").setMinTokenLength(3).setPattern("[^A-Za-z]").transform(raw)
      .select('words).map(row => row.getAs[Seq[String]]("words"))
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
    val numKeys = kvs.map(_._1).distinct.size
    val numPairs = kvs.distinct.size
    assert(numKeys == numPairs, s"$numKeys : $numPairs")
    val lookup = Map(kvs: _*)

    val documents = tfidf.zipWithIndex().map(tuple => tuple._2 -> tuple._1)

    // Set LDA parameters
    val numTopics = 8
    val lda = new LDA().setK(numTopics).setMaxIterations(15)

    val ldaModel = lda.run(documents)
//    val avgLogLikelihood = ldaModel.logLikelihood / documents.count()

    // Print topics, showing top-weighted 10 terms for each topic.
    val topicIndices = ldaModel.describeTopics(maxTermsPerTopic = 10)
    topicIndices.map(v => v._1.toSeq.map(lookup.get(_).get).zip(v._2.toSeq)).foreach(println)
//    topicIndices.foreach { case (terms, termWeights) =>
//      println("TOPIC:")
//      terms.zip(termWeights).foreach { case (term, weight) =>
//        println(s"${vocabArray(term.toInt)}\t$weight")
//      }
//      println()
//    }
  }
}