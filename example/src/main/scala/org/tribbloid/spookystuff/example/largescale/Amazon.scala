package org.tribbloid.spookystuff.example

import org.tribbloid.spookystuff.SpookyContext._
import org.tribbloid.spookystuff.entity._

import org.apache.spark.SparkContext._

object Amazon extends SparkSubmittable {

  def doMain() {

    (sc.textFile("s3n://spooky-source/wellca.tsv").distinct(40).tsvToMap("url\titem") +>
      Visit("http://www.amazon.com/") +>
      TextInput("input#twotabsearchtextbox", "#{item}") +>
      Submit("input.nav-submit-input") +>
      DelayFor("div#resultsCol",50) !).selectInto(
        "DidYouMean" -> {_.text1("div#didYouMean a") },
        "noResult" -> {_.text1("h1#noResultsTitle")}
      ).slice(
        "div[id^=result_]", limit = 5
      ).map{ page =>
    {
      page.attr1("h3 span.bold", "title")+"\t"+
        page.text1("span.red")+"\t"+
        page.attr1("a[alt$=stars]", "alt")+"\t"+
        page.text1("span.rvwCnt a")+"\t"+
        page.context.get("DidYoumean")+"\t"+
        page.context.get("noResult")
    }
    }.collect().foreach(println(_))
  }
}
