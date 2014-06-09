package org.tribbloid.spookystuff.example

import org.apache.spark.{SparkContext, SparkConf}
import org.tribbloid.spookystuff.entity._

/**
 * A more complex linkedIn job that finds name and printout skills of all Sanjay Gupta in your local area
 */
object MoreLinkedIn {

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("MoreLinkedIn")
    conf.setMaster("local[8,3]")
    conf.setSparkHome(System.getenv("SPARK_HOME"))
    conf.setJars(SparkContext.jarOfClass(this.getClass).toList)
    conf.set("spark.task.maxFailures","3")
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
    linkRDD.persist()
    val results1 = linkRDD.collect()
    results1.foreach {
      println(_)
    }

    //very important! otherwise will only use 1 thread
    val linkRDD_repart = linkRDD.repartition(20)

    //this is optional and slow, use it to release memory
//    linkRDD.unpersist()

    val actions2RDD = linkRDD_repart.map(
      link => {
        Seq[Action](
          Visit(link),
          DelayFor("div#profile-contact",50),
          Snapshot("search_result")
        )
      })

    val secondTripletRDD = actions2RDD.flatMap {
      actions => {
        PageBuilder.resolve(actions: _*)
      }
    }

//    secondTripletRDD.persist()

    val infoRDD = secondTripletRDD.map {
      triplet => {
        val action1 = triplet._1(0)
        var url: String = null

        action1 match {
          case Visit(u) => url = u

          case _ => url = "error:"+action1.toString
        }
        val page = triplet._2
        val name = page.firstText("span.full-name")
        val occupation = page.firstText("p.title")
        val skills = page.allTexts("div#profile-skills li")
        page.save(url)

        (url, name, occupation, skills)
      }
    }

    val results2 = infoRDD.collect()
    results2.foreach {
      result => println(result.toString())
    }

    sc.stop()
  }
}
