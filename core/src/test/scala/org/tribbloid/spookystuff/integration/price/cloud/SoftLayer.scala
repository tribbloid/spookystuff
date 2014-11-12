package org.tribbloid.spookystuff.integration.price.cloud

import org.tribbloid.spookystuff.actions._
import org.tribbloid.spookystuff.integration.TestCore

import scala.concurrent.duration._

/**
 * Created by peng on 23/08/14.
 */
//TODO: can't move hidden slider, fix it!
object SoftLayer extends TestCore {

  import spooky._

  override def doMain() = {
    sc.parallelize(Seq("private-local"))
      .fetch(
        Visit("http://www.softlayer.com/virtual-servers")
          +> Click("div#core-switch")
          +> Delay(1.seconds)
          *> (5 to 6).map(n => (n - 1)/5.0).map(n => DragSlider("div#private-core-slider",n))
          *> (1 to 2).map(n => (n - 1)/1.0).map(n => DragSlider("div#san-slider",n))
      )
      .extract(
        "cpu_core" -> (_.text1("span#private-core-value")),
        //        "storage" -> (_.text1("span#local-value")),
        "storage" -> (_.text1("span#san-value")),
        "memory" -> (_.text1("span#ram-value")),
        "monthly_price" -> (_.text1("a#build-monthly")),
        "hourly_price" -> (_.text1("a#build-hourly"))
      )
      .asSchemaRDD()
  }
}
//:+
//(1 to 4).map(n => (n - 1)/3.0).map(perc => Seq(Click(""), DragSlider("div#sliderCpuCores",perc)))
//)
//.map(_.actions.mkString("+>")).collect()

//      +*> (3 to 4).map(n => (n - 1)/9.0).map(n => DragSlider("div#ram-slider",n))

//      +*> (5 to 6).map(n => (n - 1)/5.0).map(n => DragSlider("div#public-core-slider",n))
//      +*> (5 to 6).map(n => (n - 1)/1.0).map(n => DragSlider("div#local-slider",n))