package com.tribbloids.spookystuff.example.encyclopedia

import com.tribbloids.spookystuff.SpookyContext
import com.tribbloids.spookystuff.actions._
import com.tribbloids.spookystuff.dsl._
import com.tribbloids.spookystuff.example.QueryCore

/**
 * Created by peng on 07/06/14.
 */
object AppliancePartsPros extends QueryCore {

  override def doMain(spooky: SpookyContext) = {
    import spooky.dsl._

    sc.parallelize(Seq("A210S"))
      .fetch(
        Visit("http://www.appliancepartspros.com/")
          +> TextInput("input.ac-input",'_)
          +> Click("input[value=\"Search\"]")
          +> WaitFor("div.dgrm-lst div.header h2")
      )
      .select(
        S"div.dgrm-lst div.header h2".text ~ 'model
      )
      .wgetJoin(S("div.inner li a:has(img)"), ordinalKey = 'schematic_index)
      .select(
        S"div#ctl00_cphMain_up1 h1".text ~ 'schematic
      )
      .wgetJoin(S("tbody.m-bsc td.pdct-descr h2 a"), ordinalKey = 'part_index)
      .select(
        S"div.m-pdct h1".text ~ 'name,
        S("div.m-pdct td[itemprop=brand]").text ~ 'brand,
        S("div.m-bsc div.mod ul li:contains(Manufacturer) strong").text ~ 'manufacturer,
        S("div.m-pdct div.m-chm p").text ~ 'replace,
        S.uri ~ 'uri
      )
      .toDF()
  }
}