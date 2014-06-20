package org.tribbloid.spookystuff.example

import java.io.Serializable

import org.apache.spark.{SparkContext, SparkConf}
import org.tribbloid.spookystuff.Conf
import org.tribbloid.spookystuff.entity._
import org.tribbloid.spookystuff.SpookyContext._

/**
* A more complex linkedIn job that finds name and printout skills of all Sanjay Gupta in your local area
*/
object MoreLinkedIn {

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("MoreLinkedIn")
    //    conf.setMaster("local[8,3]")
    conf.setMaster("local-cluster[2,4,1000]")
    conf.setSparkHome(System.getenv("SPARK_HOME"))
    val jars = SparkContext.jarOfClass(this.getClass).toList
    conf.setJars(jars)
    conf.set("spark.task.maxFailures", "3")
    val sc = new SparkContext(conf)

    Conf.init(sc)

    val actionsRDD = sc.parallelize(Seq("Sanjay", "Arun", "Hardik"))
      .+>( Visit("https://www.linkedin.com/"))
      .+>( TextInput("input#first", "#{_}"))
      .*>( TextInput("input#last", "Gupta"), TextInput("input#last", "Krishnamurthy"))
      .+>( Submit("input[name=\"search\"]"))
      .!()
      .wgetJoin("ol#result-set h2 a") //faster
      .map{ page => (
      page.text1("span.full-name"),
      page.text1("p.title"),
      page.text("div#profile-skills li")
      )
    }.collect().foreach(println(_))

    sc.stop()
  }
}
