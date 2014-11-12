package org.tribbloid.spookystuff.integration.encyclopedia

import org.tribbloid.spookystuff.actions._
import org.tribbloid.spookystuff.integration.TestCore
import org.tribbloid.spookystuff.expressions._

import scala.concurrent.duration._

/**
 * Created by peng on 07/06/14.
 */
object AppliancePartsPros extends TestCore {

  import spooky._

  override def doMain() = {
    val rdd1 = sc.parallelize(Seq("A210S"))
      .fetch(
        Visit("http://www.appliancepartspros.com/")
          +> TextInput("input.ac-input","#{_}")
          +> Click("input[value=\"Search\"]")
          +> Delay(10.seconds) //TODO: change to DelayFor to save time
      )
      .extract(
        "model" -> ( _.text1("div.dgrm-lst div.header h2") )
      ).persist()

    println(rdd1.count())

    val rdd2 = rdd1
      .wgetJoin('* href "div.inner li a:has(img)")()
      .extract("schematic" -> {_.text1("div#ctl00_cphMain_up1 h1")}).persist()

    println(rdd2.count())

    val rdd3 = rdd2
      .wgetJoin('* href "tbody.m-bsc td.pdct-descr h2 a")()
      .extract(
        "name" -> (_.text1("div.m-pdct h1")),
        "brand" ->  (_.text1("div.m-pdct td[itemprop=brand]")),
        "manufacturer" -> (_.text1("div.m-bsc div.mod ul li:contains(Manufacturer) strong")),
        "replace" -> (_.text1("div.m-pdct div.m-chm p"))
      ).persist()

    println(rdd3.count())

    rdd3.asSchemaRDD()
  }
}