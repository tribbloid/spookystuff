package org.tribbloid.spookystuff.example.encyclopedia

import org.tribbloid.spookystuff.SpookyContext
import org.tribbloid.spookystuff.actions._
import org.tribbloid.spookystuff.dsl._
import org.tribbloid.spookystuff.example.ExampleCore

/**
 * Created by peng on 07/06/14.
 */
object AppliancePartsPros extends ExampleCore {

  override def doMain(spooky: SpookyContext) = {
    import spooky._

    sc.parallelize(Seq("A210S"))
      .fetch(
        Visit("http://www.appliancepartspros.com/")
          +> TextInput("input.ac-input","'{_}")
          +> Click("input[value=\"Search\"]")
          +> WaitFor("div.dgrm-lst div.header h2")
      )
      .select(
        $"div.dgrm-lst div.header h2".text ~ 'model
      )
      .wgetJoin($("div.inner li a:has(img)"), indexKey = 'schematic_index)
      .select(
        $"div#ctl00_cphMain_up1 h1".text ~ 'schematic
      )
      .wgetJoin($("tbody.m-bsc td.pdct-descr h2 a"), indexKey = 'part_index)
      .select(
        $"div.m-pdct h1".text ~ 'name,
        $("div.m-pdct td[itemprop=brand]").text ~ 'brand,
        $("div.m-bsc div.mod ul li:contains(Manufacturer) strong").text ~ 'manufacturer,
        $("div.m-pdct div.m-chm p").text ~ 'replace,
        $.uri ~ 'uri
      )
      .asSchemaRDD()
  }
}