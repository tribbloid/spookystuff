package org.tribbloid.spookystuff.example

import org.apache.spark.{SparkContext, SparkConf}
import org.tribbloid.spookystuff.entity._

/**
 * This job will find and printout urls of all Sanjay Gupta in your local area
 */
object LinkedIn {

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("LinkedIn")
    conf.setMaster("local[*]")
    conf.setSparkHome(System.getenv("SPARK_HOME"))
    conf.setJars(SparkContext.jarOfClass(this.getClass).toList)
    val sc = new SparkContext(conf)

    val actions = Seq[Action](
      Visit("https://www.linkedin.com/"),
      Input("input#first","Sanjay"),
      Input("input#last","Gupta"),
      Submit("input[name=\"search\"]"),
      Snapshot("after_search")
    )
    val actionsRDD = sc.parallelize(Seq(actions))
    val firstTripletRDD = actionsRDD.flatMap {
      actions => {
        PageBuilder.resolve(actions: _*)
      }
    }

    val pageRDD = firstTripletRDD.map(_._2)
    val linkRDD = pageRDD.flatMap(_.page.allLinks("ol#result-set h2 a"))
    val results = linkRDD.collect()
    results.foreach {
      println(_)
    }

    sc.stop()

  }
}
