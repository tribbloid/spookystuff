package org.tribbloid.spookystuff.example

import org.tribbloid.spookystuff.entity._

/**
 * Created by peng on 07/06/14.
 */
object AppliancePartsPros extends SparkSubmittable {

  import spooky._

  override def doMain() = {
    (sc.parallelize(Seq("A210S")) +>
      Visit("http://www.appliancepartspros.com/") +>
      TextInput("input.ac-input","#{_}") +>
      Click("input[value=\"Search\"]") +>
      Delay(10) !=!() //TODO: change to DelayFor to save time
      )
      .select(
        "model" -> ( _.text1("div.dgrm-lst div.header h2") )
      )
      .wgetJoin("div.inner li a:has(img)")()
      .select("schematic" -> {_.text1("div#ctl00_cphMain_up1 h1")})
      .wgetJoin("tbody.m-bsc td.pdct-descr h2 a")()
      .select(
        "name" -> (_.text1("div.m-pdct h1")),
        "brand" ->  (_.text1("div.m-pdct td[itemprop=brand]")),
        "manufacturer" -> (_.text1("div.m-bsc div.mod ul li:contains(Manufacturer) strong")),
        "replace" -> (_.text1("div.m-pdct div.m-chm p"))
      ).asSchemaRDD()
  }
}