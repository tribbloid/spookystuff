package org.tribbloid.spookystuff.integration.scientific

import org.apache.spark.sql.SchemaRDD
import org.tribbloid.spookystuff.actions._
import org.tribbloid.spookystuff.expressions._
import org.tribbloid.spookystuff.integration.TestCore

/**
 * Created by peng on 11/2/14.
 */
object BDBioSciences extends TestCore {
  import spooky._

  override def doMain(): SchemaRDD = {

    val selectRegion = (
      WaitForDocumentReady
        +> DropDownSelect("select#region","CA")
        +> Click("input#goButton")
//        +> ExeScript(".arguments[0].click();","img.okButton")
        +> Click("img.okButton")
        +> WaitForDocumentReady
      )

    val result = noInput
      .fetch(
        Visit("http://www.bdbiosciences.com/nvCategory.jsp?action=SELECT&form=formTree_catBean&item=744667")
          +> selectRegion
          +> WaitFor("div.pane_column.pane_column_left")
      )
      .join('* href "div.pane_column.pane_column_left a" as '~)(
        Visit("#{~}")
          +> selectRegion
          +> WaitFor("div#main")
      )
      .join('* href "div#main li a:not([href^=javascript])" as '~)(
        Visit("#{~}")
          +> selectRegion
          +> WaitFor("div#container")
      )
      .extract(
        "url" -> (_.resolvedUrl),
        "leaf" -> (_.text1("div#container h1")),
        "breadcrumb" -> (_.text1("div#breadcrumb"))
      )
      .sliceJoin("table#productTable > tbody > tr:nth-of-type(n+2)")(indexKey = 'row, joinType = Inner)
      .extract(
        "Catalog" -> (_.text1(" td:nth-of-type(1)")),
        "Brand" -> (_.text1(" td:nth-of-type(2)")),
        "Name" -> (_.text1(" td:nth-of-type(3)")),
        "Size" -> (_.text1(" td:nth-of-type(4)")),
        "Documentation" -> (_.text1(" td:nth-of-type(5)")),
        "List_Price" -> (_.text1(" td:nth-of-type(6)"))
      )
      .persist()

    println(result.count())

    result.asSchemaRDD()
  }
}
