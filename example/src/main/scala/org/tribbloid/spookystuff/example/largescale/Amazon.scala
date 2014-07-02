package org.tribbloid.spookystuff.example.largescale

import org.tribbloid.spookystuff.SpookyContext._
import org.tribbloid.spookystuff.entity._
import org.tribbloid.spookystuff.example.SparkSubmittable

object Amazon extends SparkSubmittable {

  def doMain() {

    (sc.textFile("s3n://spooky-source/all.tsv").distinct(400).tsvToMap("url\titem") +>
      Visit("http://www.amazon.com/") +>
      TextInput("input#twotabsearchtextbox", "#{item}") +>
      Submit("input.nav-submit-input") +>
      DelayFor("div#resultsCol",50) !).saveAs(
        dir = "s3n://amazonsearch", overwrite = true
      ).selectInto(
        "DidYouMean" -> {_.text1("div#didYouMean a") },
        "noResult" -> {_.text1("h1#noResultsTitle")},
        "savePath" -> {_.savePath}
      ).slice(
        "div[id^=result_]:not([id$=empty])", limit = 10
      ).map{ page =>
    {
      var itemName: String = null
      if (page.attrExist("h3 span.bold", "title")) {
        itemName = page.attr1("h3 span.bold", "title")
      }
      else {
        itemName = page.text1("h3 span.bold")
      }

      page.context.get("item")+"\t"+
        itemName+"\t"+
        page.text1("span.bld")+"\t"+
        page.text1("li.sss2")+"\t"+
        page.attr1("a[alt$=stars]", "alt")+"\t"+
        page.text1("span.rvwCnt a")+"\t"+
        page.context.get("DidYouMean")+"\t"+
        page.context.get("noResult")+"\t"+
        page.context.get("savePath")
    }
    }.saveAsTextFile("s3n://amazonsearch/result")
  }
}
