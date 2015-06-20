package org.tribbloid.spookystuff.example.nlp

import java.net.URI

import org.apache.spark.mllib.feature.{HashingTF, IDF}
import org.tribbloid.spookystuff.SpookyContext
import org.tribbloid.spookystuff.actions.{Submit, TextInput, Visit}
import org.tribbloid.spookystuff.dsl._
import org.tribbloid.spookystuff.example.QueryCore

/**
 * Created by peng on 16/06/15.
 */
object Google_WordVec extends QueryCore {

  override def doMain(spooky: SpookyContext) = {
    import spooky.dsl._
    import sql.implicits._

    val tokenized = sc.parallelize("DaJiang Innovations,3D Rototics,Parrot,Airware".split(",").map(_.trim)).fetch(
        Visit("http://www.google.com/")
          +> TextInput("input[name=\"q\"]","'{_} Company")
          +> Submit("input[name=\"btnG\"]")
      ).wgetExplore(S"a:contains(next)", maxDepth = 1, depthKey = 'page, optimizer = Narrow
      ).wgetJoin(S"li.g h3.r a[href^=/url]".hrefs.andMap {
      uris =>
        uris.flatMap {
          uri =>
            new URI(uri).getQuery.split("&").find(_.startsWith("q=")).map(_.replaceAll("q=",""))
        }
    }, hasTitle = false).select(
        '$.boilerPiple ~ 'text
      ).toDF().select('_, 'text).map(r => r.getString(0) -> r.getString(1)).reduceByKey(_ + _).mapValues(
        s => s.split("[^\\w]+").toSeq.filterNot(_.length <3))

    val hashingTF = new HashingTF()
    val tf = hashingTF.transform(tokenized.values).persist()
    val idf = new IDF().fit(tf)
    val tfidf = idf.transform(tf)

    val all = tokenized.zip(tfidf).map(tuple => (tuple._1._1, tuple._1._2.distinct, tuple._2))
    val wordvec = all.map(
      tuple => (tuple._1, tuple._2.map(
        word =>(word, tuple._3(hashingTF.indexOf(word)))
      ).sortBy(- _._2))
    ).flatMapValues(v => v)

    wordvec
  }
}