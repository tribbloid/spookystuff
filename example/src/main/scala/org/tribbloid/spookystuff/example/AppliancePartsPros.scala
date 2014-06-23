package org.tribbloid.spookystuff.example

import org.apache.spark.{SparkContext, SparkConf}
import org.tribbloid.spookystuff.SpookyContext._
import org.tribbloid.spookystuff.entity._
import scala.collection.JavaConversions._
import java.io.Serializable

/**
 * Created by peng on 07/06/14.
 */
object AppliancePartsPros extends Runnable {

  override def doMain() {
    (sc.parallelize(Seq("A210S")) +>
      Visit("http://www.appliancepartspros.com/") +>
      TextInput("input.ac-input","#{_}") +>
      Click("input[value=\"Search\"]") +> //TODO: can't use Submit, why?
      Delay(10) ! //TODO: change to DelayFor to save time
      ).selectInto(
        "model" -> { _.text1("div.dgrm-lst div.header h2") }
      ).wgetJoin("div.inner li a:has(img)")
      .selectInto("schematic" -> {_.text1("div#ctl00_cphMain_up1 h1")})
      .wgetJoin("tbody.m-bsc td.pdct-descr h2 a")
      .map(
        page => (
          page.context.get("_"),
          page.context.get("model"),
          page.context.get("schematic"),
          page.text1("div.m-pdct h1"),
          page.text1("div.m-pdct td[itemprop=brand]"),
          page.text1("div.m-bsc div.mod ul li:contains(Manufacturer) strong"),
          page.text1("div.m-pdct div.m-chm p")
          )
      ).collect().foreach(println(_))

    //
    //    third.persist()
    //
    //    third.save(dir = "file:/home/peng/spookystuff/appliancedebug").foreach(println(_))
    //
    //    third
  }
}
//TODO: some tasks went dead
//TODO: some phantomjs instance doesn't close