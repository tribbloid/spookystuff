package org.tribbloid.spookystuff.integration.scientific

import org.apache.spark.sql.SchemaRDD
import org.tribbloid.spookystuff.entity.client._
import org.tribbloid.spookystuff.integration.TestCore

/**
 * Created by peng on 11/1/14.
 */
object EmdMilliPore extends TestCore {
  import spooky._

  override def doMain(): SchemaRDD = {
    (sc.parallelize(Seq("http://www.emdmillipore.com/Web-US-Site/en_CA/-/USD/ViewParametricSearch-Browse?SynchronizerToken=fefc4cb500c0abec1ee06fbe81e8c4aa026fb8a944d4b54af79d0166ca4b1fa8&TrackingSearchType=filter&SearchTerm=*&SelectedSearchResult=SFProductSearch&SearchParameter=%26%40QueryTerm%3D*%26channels%3DUS_or_GLOBAL%26ContextCategoryUUIDs%3DXKOb.qB.JfsAAAE_3wp3.Lxj%26MERCK_FF.defaultSimilarity%3D9000&PageNumber=0&SortingAttribute=&PageSize=200"
    ),1)
      +> Visit("#{_}")
      !=!())
      .sliceJoin("section.product")(indexKey = "product_row")
      .extract(
        "Product name" -> (_.text1("h2")),
        "Description" -> (_.text1("div.container-serp > div"))
      )
      .asSchemaRDD()
  }
}
