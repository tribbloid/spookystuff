package org.tribbloid.spookystuff.example

import org.apache.spark.{SparkContext, SparkConf}
import org.tribbloid.spookystuff.{SparkSubmittable, Conf}
import org.tribbloid.spookystuff.entity._
import org.tribbloid.spookystuff.SpookyContext._
import java.io.Serializable

/**
 * This job will find and printout urls of Sanjay Gupta, Arun Gupta and Hardik Gupta in your area
 */
object LinkedIn extends SparkSubmittable {

  def doMain() {

    (sc.parallelize(Seq("Sanjay", "Arun", "Hardik")) +>
      Visit("https://www.linkedin.com/") +>
      TextInput("input#first","#{_}") +>
      TextInput("input#last","Gupta") +>
      Submit("input[name=\"search\"]") !)
      .map {page => page.href("ol#result-set h2 a").asInstanceOf[Serializable]} //TODO: How to avoid this tail?
      .collect().foreach{
      value => {
        println("-------------------------------")
        value.asInstanceOf[Seq[String]].foreach( println(_) )
      }
    }
  }
}
