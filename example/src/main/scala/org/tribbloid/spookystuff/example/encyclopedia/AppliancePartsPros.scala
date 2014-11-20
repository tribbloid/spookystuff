package org.tribbloid.spookystuff.example.encyclopedia

import org.tribbloid.spookystuff.SpookyContext
import org.tribbloid.spookystuff.actions._
import org.tribbloid.spookystuff.example.ExampleCore
import org.tribbloid.spookystuff.expressions._

import scala.concurrent.duration._

/**
 * Created by peng on 07/06/14.
 */
object AppliancePartsPros extends ExampleCore {

  override def doMain(spooky: SpookyContext) = {
    import spooky._

    sc.parallelize(Seq("A210S"))
      .fetch(
        Visit("http://www.appliancepartspros.com/")
          +> TextInput("input.ac-input","#{_}")
          +> Click("input[value=\"Search\"]")
          +> Delay(10.seconds) //TODO: change to DelayFor to save time
      )
      .extract(
        "model" -> ( _.text1("div.dgrm-lst div.header h2") )
      )
      .wgetJoin('* href "div.inner li a:has(img)", indexKey = 'schematic_index)()
      .extract(
        "schematic" -> ( _.text1("div#ctl00_cphMain_up1 h1") )
      )
      .wgetJoin('* href "tbody.m-bsc td.pdct-descr h2 a", indexKey = 'part_index)()
      .extract(
        "name" -> (_.text1("div.m-pdct h1")),
        "brand" ->  (_.text1("div.m-pdct td[itemprop=brand]")),
        "manufacturer" -> (_.text1("div.m-bsc div.mod ul li:contains(Manufacturer) strong")),
        "replace" -> (_.text1("div.m-pdct div.m-chm p"))
      )
      .asSchemaRDD()
  }
}