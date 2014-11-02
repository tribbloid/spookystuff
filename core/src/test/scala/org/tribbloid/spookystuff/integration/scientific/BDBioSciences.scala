package org.tribbloid.spookystuff.integration.scientific

import org.apache.spark.sql.SchemaRDD
import org.tribbloid.spookystuff.entity.client._
import org.tribbloid.spookystuff.integration.TestCore

/**
 * Created by peng on 11/2/14.
 */
object BDBioSciences extends TestCore {
  import spooky._

  override def doMain(): SchemaRDD = {

    val firstPages = (noInput
      +> Visit("http://www.bdbiosciences.com/nvCategory.jsp?action=SELECT&form=formTree_catBean&item=744667")
      +> DelayForDocumentReady
      +> DelayFor("div.pane_column.pane_column_left a")
      !=!())
      .wgetJoin("div.pane_column.pane_column_left a")()
      .wgetJoin("li a:not([href^=javascript])")()
      .extract(
        "url" -> (_.resolvedUrl),
        "leaf" -> (_.text1("h1")),
        "breadcrumb" -> (_.text1("div#breadcrumb"))
      )
      .persist()

    println(firstPages.count())

    firstPages.asSchemaRDD()
  }
}
