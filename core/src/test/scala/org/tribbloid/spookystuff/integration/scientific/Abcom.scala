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
    (noInput
      +> Visit("http://www.abcam.com/products")
      !=!())
      .sliceJoin("li.first_menu ul.pws_menu_level3 > li > a.level2-link")(indexKey = "category_index")
      .extract(
        "category" -> (_.text1("*"))
      )
      .wgetJoin("*")()
      .paginate("li.nextLink > a")(indexKey = "page")
      .sliceJoin("div.pws-item-info")(indexKey = "product_row", joinType = Inner)
      .extract(
        "name" -> (_.text1("h3 a")),
        "description" -> (_.text1("div.pws_item.ProductDescription div.pws_value")),
        "application" -> (_.text1("div.pws_item.Application div.pws_value"))
      )
      .asSchemaRDD()
  }
}