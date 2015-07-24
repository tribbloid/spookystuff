package org.tribbloid.spookystuff.example.ml

import org.apache.spark.ml.feature.RegexTokenizer
import org.apache.spark.mllib.feature.{HashingTF, IDF}
import org.tribbloid.spookystuff.SpookyContext
import org.tribbloid.spookystuff.dsl._
import org.tribbloid.spookystuff.example.{Common, QueryCore}
import org.tribbloid.spookystuff.http.HttpUtils

object Google_TFIDF extends QueryCore {

  override def doMain(spooky: SpookyContext) = {
    import spooky.dsl._
    import sql.implicits._

    val raw = sc.parallelize("Dell, DJI, Starbucks, McDonald".split(",").map(_.trim)).fetch(
      Common.googleSearch('_)
    ).wgetExplore(S"a:contains(next)", maxDepth = 1, depthKey = 'page, optimizer = Narrow
      ).wgetJoin(S"li.g h3.r a".hrefs.flatMap {
      uri =>
        val query = HttpUtils.uri(uri).getQuery
        val realURI = if (query == null) Some(uri)
        else if (uri.contains("/url?")) query.split("&").find(_.startsWith("q=")).map(_.replaceAll("q=",""))
        else None
        realURI
    },failSafe = 2 ).select(
        '$.boilerPipe.orElse(Some("")) ~ 'text
      ).toDF()

    val tokenized = new RegexTokenizer().setInputCol("text").setOutputCol("words").setMinTokenLength(3).setPattern("[^A-Za-z]").transform(raw)
      .select('_, 'words).map(row => row.getAs[String]("_") -> row.getAs[Seq[String]]("words")).reduceByKey(_ ++ _)
    val stemmed = tokenized.mapValues(_.map(v => v.toLowerCase))
//    val stemmed = tokenized.mapValues(_.map(v => PorterStemmer.apply(v.toLowerCase)))

    val hashingTF = new HashingTF()
    val tf = hashingTF.transform(stemmed.values).persist()
    val idf = new IDF().fit(tf)
    val tfidf = idf.transform(tf)

    val all = stemmed.zip(tfidf).map(tuple => (tuple._1._1, tuple._1._2.distinct, tuple._2))
    val wordvec = all.map(
      tuple => (tuple._1, tuple._2.map(
        word =>(word, tuple._3(hashingTF.indexOf(word)))
      ).sortBy(- _._2))
    ).mapValues(_.slice(0,10)).flatMapValues(v => v)

    val df = wordvec.map(v => EntityKeyword(v._1, v._2._1, v._2._2.toString))
    df.toDF()
  }
}