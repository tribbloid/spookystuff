package org.tribbloid.spookystuff.integration.social

import org.tribbloid.spookystuff.actions._
import org.tribbloid.spookystuff.integration.TestCore

/**
 * This job will find and printout urls of Sanjay Gupta, Arun Gupta and Hardik Gupta in your area
 */
//TODO: LinkedIn is down
object LinkedInSimple extends TestCore {

  import spooky._

  def doMain() = {

    //    (sc.parallelize(Seq("Sanjay", "Arun", "Hardik"))
    (sc.parallelize(Seq("Sanjay"))
      +> Visit("https://www.linkedin.com/")
      //      +> TextInput("input#first","#{_}")
      //      +> TextInput("input#last","Gupta")
      //      +> Submit("input[name=\"search\"]")
      !=!())
      //      .select("links" -> (_.href("ol#result-set h2 a").mkString("\t")))
      .asSchemaRDD()
  }
}
