package org.tribbloid.spookystuff.acceptance.cloud

import org.tribbloid.spookystuff.SpookyContext._
import org.tribbloid.spookystuff.acceptance.SparkTestCore
import org.tribbloid.spookystuff.entity._

/**
 * Created by peng on 23/08/14.
 */
object Peer1 extends SparkTestCore {

  override def doMain(): Array[_] = {
    ((sc.parallelize(Seq("Dummy")) +>
      Visit("http://www.peer1.ca/cloud-hosting/mission-critical-cloud") +*>
      (1 to 3).map(n => (n - 1)/15.0).map(DragSlider("div#sliderCpuCores",_)) +*>
      (1 to 4).map(n => (n - 1)/127.0).map(DragSlider("div#sliderMemory",_)) +*>
      (17.to(37,10)).map(n => (n - 17)/1007.0).map(DragSlider("div#sliderCloudStorage",_))).repartition(24) !==)
      .selectInto (
      "cpu_core" -> (_.text1("span#previewCpuCores")),
      "memory" -> (_.text1("span#previewMemory")),
      "storage" -> (_.text1("span#previewCloudStorage")),
      "monthly_price" -> (_.text1("span#monthly")),
      "hourly_price" -> (_.text1("span#hourly"))
    )
      .asJsonRDD()
      .collect()
  }
}
