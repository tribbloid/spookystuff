package org.tribbloid.spookystuff.example

import java.io.Serializable

import org.apache.spark.{SparkContext, SparkConf}
import org.tribbloid.spookystuff.entity._
import org.tribbloid.spookystuff.SpookyContext._

/**
* A more complex linkedIn job that finds name and printout skills of all Sanjay Gupta in your local area
*/
object MoreLinkedIn {

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("MoreLinkedIn")
    conf.setMaster("local[8,3]")
    //    conf.setMaster("local-cluster[2,4,1000]")
//    conf.setSparkHome(System.getenv("SPARK_HOME"))
//    val jars = SparkContext.jarOfClass(this.getClass).toList
//    conf.setJars(jars)
    conf.set("spark.task.maxFailures", "3")
    val sc = new SparkContext(conf)

    val actionsRDD = sc.parallelize(Seq("Sanjay", "Arun", "Hardik")) +>
      Visit("https://www.linkedin.com/") +>
      TextInput("input#first", "#{_}") +>
      TextInput("input#last", "Gupta") +>
      Submit("input[name=\"search\"]")

    val pageRDD = actionsRDD !()

    //this is just for demoing multi-stage job
//    pageRDD.persist()
//    val linkRDD = pageRDD.selectAll[String] {
//      _.linkAll("ol#result-set h2 a")
//    }
//    linkRDD.collect().foreach{ println(_) }

    val personalPageRDD = pageRDD.fork("ol#result-set h2 a")

    val resultRDD = personalPageRDD.select{ page => (
      page.textFirst("span.full-name"),
      page.textFirst("p.title"),
      page.textAll("div#profile-skills li")
      )
    }

    resultRDD.collect().foreach(println(_))

    sc.stop()
  }
}
