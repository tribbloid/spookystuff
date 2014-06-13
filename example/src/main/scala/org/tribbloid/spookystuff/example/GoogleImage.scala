//package org.tribbloid.spookystuff.example
//
//import org.apache.spark.{SparkContext, SparkConf}
//import org.tribbloid.spookystuff.entity._
//
///**
// * Created by peng on 10/06/14.
// */
//object GoogleImage {
//
//  def main(args: Array[String]) {
//    val conf = new SparkConf().setAppName("MoreLinkedIn")
//    conf.setMaster("local[8,3]")
//    //    conf.setMaster("local-cluster[2,4,1000]")
//    conf.setSparkHome(System.getenv("SPARK_HOME"))
//    val jars = SparkContext.jarOfClass(this.getClass).toList
//    conf.setJars(jars)
//    conf.set("spark.task.maxFailures", "3")
//    val sc = new SparkContext(conf)
//
//    val nameRDD = sc.textFile("/home/peng/Documents/affiliation.txt")
////    val nameRDD = sc.textFile("/home/peng/Documents/affiliation_short5.txt")
//
//    val actionsRDD = nameRDD.map(
//      name => {
//        Seq[Action](
//          Visit("http://images.google.com/"),
//          DelayFor("form[action=\"/search\"]",50),
//          Input("input[name=\"q\"]",name + " Logo"),
//          Submit("input[name=\"btnG\"]"),
//          DelayFor("div#search img",50),
//          Snapshot() //better safe than sorry
//        )
//      }
//    )
//
//    val pageRDD = actionsRDD.flatMap(
//      actions => {
//        PageBuilder.resolve(actions:_*)
//      }
//    )
//
//    val nameAndSrcRDD = pageRDD.map(
//      pages => {
//        (pages._2.values.get("file_name").get(0) , pages._2.page.firstAttr("div#search img","src"))
//      }
//    )
//
////    nameAndSrcRDD.persist()
////    val results1 = nameAndSrcRDD.collect()
////    results1.foreach {
////      result => println(result.toString())
////    }
//
//    val actions2RDD = nameAndSrcRDD.map(
//      nameSrc => {
//        val actions = Seq[Action](
//          Wget(nameSrc._2,nameSrc._1)
//        )
//        PageBuilder.resolve(actions:_*)
//        nameSrc._2
//      }
//    )
//
//    val results2 = actions2RDD.collect()
//    results2.foreach {
//      result => println(result.toString())
//    }
//
//    sc.stop()
//  }
//}
