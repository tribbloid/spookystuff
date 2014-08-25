package org.tribbloid.spookystuff.acceptance.cloud

import org.tribbloid.spookystuff.SpookyContext._
import org.tribbloid.spookystuff.acceptance.SparkTestCore
import org.tribbloid.spookystuff.entity._
import scala.collection.JavaConversions._

/**
 * Created by peng on 23/08/14.
 */
object Softlayer2 extends SparkTestCore {

  override def doMain(): Array[_] = {
    ((sc.parallelize(Seq("private-local")) +>
      Visit("http://www.softlayer.com/virtual-servers") +*>
      (1 to 10).map(n => (n - 1)/9.0).map(DragSlider("div#ram-slider",_)) +*>
      (1 to 6).map(n => (n - 1)/5.0).map(DragSlider("div#private-core-slider",_,handleSelector = "a.ui-slider-handle")) +*>
      (1 to 2).map(n => (n - 1)/1.0).map(DragSlider("div#local-slider",_))
      )!><)
      .selectInto (
      "cpu_core" -> (_.text1("span#private-core-value")),
      "storage" -> (_.text1("span#local-value")),
      "memory" -> (_.text1("span#ram-value")),
      "monthly_price" -> (_.text1("a#build-monthly")),
      "hourly_price" -> (_.text1("a#build-hourly"))
    )//.saveAs(dir="file:///home/peng/spookystuff/softlayer")
      .asJsonRDD()
      .collect()
  }
}

//:+
//(1 to 4).map(n => (n - 1)/3.0).map(perc => Seq(Click(""), DragSlider("div#sliderCpuCores",perc)))
//)
//.map(_.actions.mkString("+>")).collect()