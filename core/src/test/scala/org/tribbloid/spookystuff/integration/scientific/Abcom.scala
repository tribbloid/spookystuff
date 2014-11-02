package org.tribbloid.spookystuff.integration.scientific

import org.apache.spark.sql.SchemaRDD
import org.tribbloid.spookystuff.entity.client._
import org.tribbloid.spookystuff.integration.TestCore
import org.tribbloid.spookystuff.operator._

/**
 * Created by peng on 10/31/14.
 */
object Abcom extends TestCore {

  import spooky._

  override def doMain(): SchemaRDD = {
    val firstPages = (noInput
      +> Visit("http://www.abcam.com/products")
      !=!())
      .sliceJoin("li.first_menu ul.pws_menu_level3 li:not(:has(ul)) a.level2-link")(indexKey = "category_index")
      .extract(
        "category" -> (_.text1("*"))
      )
      .wgetJoin("*")(numPartitions = 300).persist()

    println(firstPages.count())

    val allPages = firstPages
      .paginate("li.nextlink[class!=disabled] > a[class!=disabled]")(indexKey = "page")
      .repartition(1000).persist()

    println(allPages.count())

    val rdd = allPages
      .sliceJoin("div.pws-item-info")(indexKey = "product_row", joinType = Inner)
      .extract(
        "name" -> (_.text1("h3 a")),
        "description" -> (_.text1("div.pws_item.ProductDescription div.pws_value")),
        "application" -> (_.text1("div.pws_item.Application div.pws_value"))
      ).persist()

    println(rdd.count())

    rdd.asSchemaRDD()
  }
}