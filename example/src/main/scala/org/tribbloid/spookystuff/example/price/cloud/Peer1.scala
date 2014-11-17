package org.tribbloid.spookystuff.example.price.cloud

import org.tribbloid.spookystuff.SpookyContext
import org.tribbloid.spookystuff.actions._
import org.tribbloid.spookystuff.example.ExampleCore

/**
 * Created by peng on 23/08/14.
 */
object Peer1 extends ExampleCore {

  override def doMain(spooky: SpookyContext) = {
    import spooky._
    noInput
      .fetch(
        Visit("http://www.peer1.ca/cloud-hosting/mission-critical-cloud")
          *> (1 to 3).map(n => (n - 1)/15.0).map(DragSlider("div#sliderCpuCores",_))
          *> (1 to 4).map(n => (n - 1)/127.0).map(DragSlider("div#sliderMemory",_))
          *> (1 to 3).map(n => (n - 1)/99.0).map(DragSlider("div#sliderCloudStorage",_))
      )
      .extract (
        "cpu_core" -> (_.text1("span#previewCpuCores")),
        "memory" -> (_.text1("span#previewMemory")),
        "storage" -> (_.text1("span#previewCloudStorage")),
        "monthly_price" -> (_.text1("span#monthly")),
        "hourly_price" -> (_.text1("span#hourly"))
      )
      .asSchemaRDD()
  }
}
