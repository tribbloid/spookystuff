package org.tribbloid.spookystuff.acceptance

import org.tribbloid.spookystuff.SpookyContext._
import org.tribbloid.spookystuff.entity._

/**
 * This job will find and printout urls of Sanjay Gupta, Arun Gupta and Hardik Gupta in your area
 */
object LinkedInSimple extends AcceptanceTestCore {

  def doMain() = {

    (sc.parallelize(Seq("Sanjay", "Arun", "Hardik")) +>
      Visit("https://www.linkedin.com/") +>
      TextInput("input#first","#{_}") +>
      TextInput("input#last","Gupta") +>
      Submit("input[name=\"search\"]") !)
      .map {page => page.href("ol#result-set h2 a")}
      .collect()
  }
}
