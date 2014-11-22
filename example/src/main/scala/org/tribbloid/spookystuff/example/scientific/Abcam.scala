package org.tribbloid.spookystuff.example.scientific

import org.tribbloid.spookystuff.SpookyContext
import org.tribbloid.spookystuff.actions._
import org.tribbloid.spookystuff.example.ExampleCore
import org.tribbloid.spookystuff.expressions._

/**
 * Created by peng on 10/31/14.
 */
object Abcam extends ExampleCore {

  override def doMain(spooky: SpookyContext) = {
    import spooky._

    val firstPages = noInput
      .fetch(
        Visit("http://www.abcam.com/products")
      )
      .sliceJoin("li.first_menu ul.pws_menu_level3 li:not(:has(ul)) a.level2-link")(indexKey = 'category_index)
      .extract(
        "category" -> (_.text1("*"))
      )
      .wgetJoin('* href "*")(numPartitions = 300).persist()

    println(firstPages.count())

    val allPages = firstPages
      .wgetExplore('* href "li.nextlink[class!=disabled] > a[class!=disabled]")(depthKey = 'page)
      .repartition(1000).persist()

    println(allPages.count())

    val rdd = allPages
      .extract(
        "url" -> (_.url)
      )
      .sliceJoin("div.pws-item-info")(indexKey = 'product_row, joinType = Inner)
      .extract(
        "name" -> (_.text1("h3 a")),
        "description" -> (_.text1("div.pws_item.ProductDescription div.pws_value")),
        "application" -> (_.text1("div.pws_item.Application div.pws_value"))
      ).persist()

    println(rdd.count())

    rdd.asSchemaRDD()
  }
}