package org.tribbloid.spookystuff.integration.price

import org.tribbloid.spookystuff.actions._
import org.tribbloid.spookystuff.integration.TestCore

object Amazon extends TestCore {

  import spooky._

  override def doMain() = {

    (sc.parallelize(Seq("http://dummy.com\tLord of the Rings\t3.0"))
      .tsvToMap("url\titem\tiherb-price")
      +> Visit("http://www.amazon.com/")
      +> TextInput("input#twotabsearchtextbox", "#{item}")
      +> Submit("input.nav-submit-input")
      +> DelayFor("div#resultsCol")
      !=!())
      .extract(
        "DidYouMean" -> {_.text1("div#didYouMean a") },
        "noResultsTitle" -> {_.text1("h1#noResultsTitle")},
        "savePath" -> {_.saved}
      )
      .sliceJoin("div.prod[id^=result_]:not([id$=empty])")(limit = 10)
      .extract(
        "item_name" -> (page => Option(page.attr1("h3 span.bold", "title")).getOrElse(page.text1("h3 span.bold"))),
        "price" -> (_.text1("span.bld")),
        "shipping" -> (_.text1("li.sss2")),
        "stars" -> (_.attr1("a[alt$=stars]", "alt")),
        "num_rating" -> (_.text1("span.rvwCnt a"))
      )
      .asSchemaRDD()
  }
}