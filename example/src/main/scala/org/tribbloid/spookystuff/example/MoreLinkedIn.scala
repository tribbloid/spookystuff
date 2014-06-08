package org.tribbloid.spookystuff.example

import org.apache.spark.{SparkContext, SparkConf}
import org.tribbloid.spookystuff.entity._

/**
 * A more complex linkedIn job that finds name and printout skills of all Sanjay Gupta in your local area
 */
object MoreLinkedIn {

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("MoreLinkedIn")
    conf.setMaster("local[*]")
    conf.setSparkHome(System.getenv("SPARK_HOME"))
    conf.setJars(SparkContext.jarOfClass(this.getClass).toList)
    val sc = new SparkContext(conf)

    val actions = Seq[Action](
      Visit("https://www.linkedin.com/"),
      Input("input#first","Sanjay"),
      Input("input#last","Gupta"),
      Submit("input[name=\"search\"]"),
      Snapshot("search_result")
    )
    val actionsRDD = sc.parallelize(Seq(actions))
    val firstTripletRDD = actionsRDD.flatMap {
      actions => {
        PageBuilder.resolve(actions: _*)
      }
    }

    val pageRDD = firstTripletRDD.map(_._2)
    val linkRDD = pageRDD.flatMap(_.allLinks("ol#result-set h2 a"))

    //this is just for demoing multi-stage job
    //TODO: whats the difference between persist() and cache()?
    linkRDD.persist()
    val results1 = linkRDD.collect()
    results1.foreach {
      println(_)
    }

    //very important! otherwise will only use 1 thread
    val linkRDD_repart = linkRDD.repartition(8)

    val actions2RDD = linkRDD_repart.map(
      link => {
        Seq[Action](
          Visit(link),
          Snapshot("search_result")
        )
      })

    val secondTripletRDD = actions2RDD.flatMap {
      actions => {
        PageBuilder.resolve(actions: _*)
      }
    }

    val infoRDD = secondTripletRDD.map {
      triplet => {
        val page = triplet._2
        val name = page.firstText("span.full-name")
        val occupation = page.firstText("p.title")
        val skills = page.allTexts("div#profile-skills li")
        (name, occupation, skills)
      }
    }

    val results2 = infoRDD.collect()
    results2.foreach {
      result => println(result.toString())
    }
  }
}
