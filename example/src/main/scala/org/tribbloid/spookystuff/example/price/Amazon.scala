package org.tribbloid.spookystuff.example.price

import org.tribbloid.spookystuff.SpookyContext
import org.tribbloid.spookystuff.actions._
import org.tribbloid.spookystuff.example.ExampleCore

object Amazon extends ExampleCore {

  override def doMain(spooky: SpookyContext) = {
    import spooky._

    sc.parallelize(Seq("http://dummy.com\tLord of the Rings\t3.0"))
      .tsvToMap("url\titem\tiherb-price")
      .fetch(
        Visit("http://www.amazon.com/")
          +> TextInput("input#twotabsearchtextbox", "#{item}")
          +> Submit("input[type=submit]")
          +> WaitFor("div#resultsCol")
      )
      .extract(
        "DidYouMean" -> {_.text1("div#didYouMean a") },
        "noResultsTitle" -> {_.text1("h1#noResultsTitle")},
        "savePath" -> {_.saved}
      )
      .sliceJoin("div.s-item-container > div.a-fixed-left-grid > div.a-fixed-left-grid-inner")(limit = 10)
      .extract(
        "item_name" -> (page => Option(page.attr1("a.s-access-detail-page", "title")).getOrElse(page.text1("a.s-access-detail-page"))),
        "price" -> (_.text1("span.a-size-base.s-price")),
        "stars" -> (_.text1("span.a-icon-alt")),
        "num_rating" -> (_.text1("div.a-column.a-span5 > div.a-row > a.a-size-small"))
      )
      .asSchemaRDD()
  }
}