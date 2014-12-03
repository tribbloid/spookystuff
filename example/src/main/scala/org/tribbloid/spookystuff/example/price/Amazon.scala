package org.tribbloid.spookystuff.example.price

import org.tribbloid.spookystuff.{dsl, SpookyContext}
import org.tribbloid.spookystuff.actions._
import org.tribbloid.spookystuff.example.ExampleCore
import dsl._

object Amazon extends ExampleCore {

  override def doMain(spooky: SpookyContext) = {
    import spooky._

    sc.parallelize(Seq("http://dummy.com\tLord of the Rings\t3.0"))
      .tsvToMap("url\titem\tiherb-price")
      .fetch(
        Visit("http://www.amazon.com/")
          +> TextInput("input#twotabsearchtextbox", 'item)
          +> Submit("input[type=submit]")
          +> WaitFor("div#resultsCol")
      )
      .select(
        $"div#didYouMean a".text > 'DidYouMean,
        $"h1#noResultsTitle".text > 'noResultsTitle,
        '*.saved > 'savePath
      )
      .flatSelect($"div.s-item-container > div.a-fixed-left-grid > div.a-fixed-left-grid-inner") (
        A"a.s-access-detail-page".head.andFlatMap(element => element.attr("title").orElse(element.text)) > 'item_name,
        A"span.a-size-base.s-price".text > 'price,
        A"span.a-icon-alt".text > 'stars,
        A"div.a-column.a-span5 > div.a-row > a.a-size-small".text > 'num_rating
      )
      .asSchemaRDD()
  }
}