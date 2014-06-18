package org.tribbloid.spookystuff.example

import org.apache.spark.{SparkContext, SparkConf}
import org.tribbloid.spookystuff.entity._
import org.tribbloid.spookystuff.SpookyContext._
import java.io.Serializable

/**
 * This job will find and printout urls of Sanjay Gupta, Arun Gupta and Hardik Gupta in your area
 */
object LinkedIn {

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("LinkedIn")
    conf.setMaster("local[*]")
    conf.setSparkHome(System.getenv("SPARK_HOME"))
    conf.setJars(SparkContext.jarOfClass(this.getClass).toList)
    val sc = new SparkContext(conf)

    val actionsRDD = sc.parallelize(Seq("Sanjay", "Arun", "Hardik")) +>
      Visit("https://www.linkedin.com/") +>
      TextInput("input#first","#{_}") +>
      TextInput("input#last","Gupta") +>
      Submit("input[name=\"search\"]")

//    val action1 = actionsRDD.first()

    val pageRDD = actionsRDD !

    val valueRDD = pageRDD.selectInto(
      "link" -> {
        page: HtmlPage => page.linkAll("ol#result-set h2 a").asInstanceOf[Serializable] //TODO: How to avoid this tail?
      }
    )

    valueRDD.collect().foreach{
      map => {
        println("-------------------------------")
        map.get("link").asInstanceOf[Seq[String]].foreach( println(_) )
      }
    }

    sc.stop()

  }
}
