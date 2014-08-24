package org.tribbloid.spookystuff.acceptance.cloud

import org.tribbloid.spookystuff.SpookyContext._
import org.tribbloid.spookystuff.acceptance.SparkTestCore
import org.tribbloid.spookystuff.entity._

/**
 * Created by peng on 23/08/14.
 */
object Zunicore extends SparkTestCore {

  override def doMain(): Array[_] = {
    (
      sc.parallelize(Seq("Dummy")) +>
        Visit("https://www.zunicore.com/") +*>
        (1 to 2).map(n => (n - 1)/11.0).map(DragSlider("div#6_slider",_)) +*>
        (1 to 4).map(n => (n - 1)/31.0).map(DragSlider("div#7_slider",_)) +*>
        (1 to 3).map(n => (n - 1)/19.0).map(DragSlider("div#8_slider",_)) !==
      )
      .selectInto (
      "total_price" -> (_.text1("div#5_total_price")),
      "hourly_price" -> (_.text1("div#5_total_price_hourly"))
    )
      .collect()
  }
}
