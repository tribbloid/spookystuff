package org.tribbloid.spookystuff.example.social

import org.tribbloid.spookystuff.{dsl, SpookyContext}
import org.tribbloid.spookystuff.actions._
import org.tribbloid.spookystuff.example.ExampleCore
import dsl._

/**
 * This job will find and printout urls of Sanjay Gupta, Arun Gupta and Hardik Gupta in your area
 */
//TODO: LinkedIn is down
object LinkedInSimple extends ExampleCore {

  override def doMain(spooky: SpookyContext) = {
    import spooky._

    //    (sc.parallelize(Seq("Sanjay", "Arun", "Hardik"))
    sc.parallelize(Seq("Sanjay"))
      .fetch(
        Visit("https://www.linkedin.com/")
      )
      //      +> TextInput("input#first","#{_}")
      //      +> TextInput("input#last","Gupta")
      //      +> Submit("input[name=\"search\"]")
      //      .select("links" -> (_.href("ol#result-set h2 a").mkString("\t")))
      .asSchemaRDD()
  }
}
