//package org.tribbloid.spookystuff.acceptance.cloud
//
//import org.tribbloid.spookystuff.SpookyContext._
//import org.tribbloid.spookystuff.acceptance.SpookyTestCore
//import org.tribbloid.spookystuff.entity._
//
///**
// * Created by peng on 23/08/14.
// */
//object Softlayer extends SpookyTestCore {
//
//  override def doMain(): Array[_] = {
//    (sc.parallelize(Seq("public-san")) +>
//      Visit("http://www.softlayer.com/virtual-servers") +*>
//      (1 to 6).map(n => (n - 1)/5.0).map(DragSlider("div#public-core-slider",_)) +*>
//      (1 to 10).map(n => (n - 1)/9.0).map(DragSlider("div#ram-slider",_)) +*>
//      (1 to 2).map(n => (n - 1)/1.0).map(DragSlider("div#san-slider",_))!><)
//      .selectInto (
//      "cpu_core" -> (_.text1("span#public-core-value")),
//      "storage" -> (_.text1("span#san-value"))
//    )
//      .union(
//        (sc.parallelize(Seq("private-san")) +>
//          Visit("http://www.softlayer.com/virtual-servers") +>
//          Click("div#core-switch") +>
//          Delay(2) +*>
//          (1 to 4).map(n => (n - 1)/3.0).map(DragSlider("div#private-core-slider",_)) +*>
//          (1 to 10).map(n => (n - 1)/9.0).map(DragSlider("div#ram-slider",_)) +*>
//          (1 to 2).map(n => (n - 1)/1.0).map(DragSlider("div#san-slider",_))!><)
//          .selectInto (
//          "cpu_core" -> (_.text1("span#private-core-value")),
//          "storage" -> (_.text1("span#san-value"))
//        )
//      )
//      .union(
//        (sc.parallelize(Seq("public-local")) +>
//          Visit("http://www.softlayer.com/virtual-servers") +>
//          Click("div#storage-switch") +>
//          Delay(2) +*>
//          (1 to 6).map(n => (n - 1)/5.0).map(DragSlider("div#public-core-slider",_)) +*>
//          (1 to 10).map(n => (n - 1)/9.0).map(DragSlider("div#ram-slider",_)) +*>
//          (1 to 2).map(n => (n - 1)/1.0).map(DragSlider("div#local-slider",_))!><)
//          .selectInto (
//          "cpu_core" -> (_.text1("span#public-core-value")),
//          "storage" -> (_.text1("span#local-value"))
//        )
//      )
//      .union(
//        (sc.parallelize(Seq("private-local")) +>
//          Visit("http://www.softlayer.com/virtual-servers") +>
//          Click("div#core-switch") +>
//          Click("div#storage-switch") +>
//          Delay(2) +*>
//          (1 to 4).map(n => (n - 1)/3.0).map(DragSlider("div#private-core-slider",_)) +*>
//          (1 to 10).map(n => (n - 1)/9.0).map(DragSlider("div#ram-slider",_)) +*>
//          (1 to 2).map(n => (n - 1)/1.0).map(DragSlider("div#local-slider",_))!><)
//          .selectInto (
//          "cpu_core" -> (_.text1("span#private-core-value")),
//          "storage" -> (_.text1("span#local-value"))
//        )
//      )
//      .selectInto (
//      "memory" -> (_.text1("span#ram-value")),
//      "monthly_price" -> (_.text1("a#build-monthly")),
//      "hourly_price" -> (_.text1("a#build-hourly"))
//    )
//      .map(page => {
//      page.context.put("type", page.context.get("_"))
//      page.context.remove("_")
//      page
//    })
//      .asJsonRDD()
//      .collect()
//  }
//}
//
////:+
////(1 to 4).map(n => (n - 1)/3.0).map(perc => Seq(Click(""), DragSlider("div#sliderCpuCores",perc)))
////)