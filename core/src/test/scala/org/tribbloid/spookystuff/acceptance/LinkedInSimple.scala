package org.tribbloid.spookystuff.acceptance

import org.tribbloid.spookystuff.entity._

/**
 * This job will find and printout urls of Sanjay Gupta, Arun Gupta and Hardik Gupta in your area
 */
//TODO: LinkedIn is down
object LinkedInSimple extends SpookyTestCore {

  import spooky._

  def doMain() = {

    (sc.parallelize(Seq("Sanjay", "Arun", "Hardik"))
      +> Visit("https://www.linkedin.com/")
//      +> TextInput("input#first","#{_}")
//      +> TextInput("input#last","Gupta")
//      +> Submit("input[name=\"search\"]")
      !=!())
//      .select("links" -> (_.href("ol#result-set h2 a").mkString("\t")))
      .saveAs(dir = "file:///home/peng/spookystuff/LinkedInSimple/")
      .asSchemaRDD()
  }
}
