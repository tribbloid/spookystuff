package org.tribbloid.spookystuff.acceptance

import org.tribbloid.spookystuff.entity._

/**
 * A more complex linkedIn job that finds name and printout skills of all Sanjay Gupta in your local area
 */
//remember infix operator cannot be written in new line
object LinkedIn extends SpookyTestCore {

  import spooky._

  def doMain() = {

    (sc.parallelize(Seq("Sanjay", "Arun", "Hardik")) +>
      Visit("https://www.linkedin.com/") +>
      TextInput("input#first", "#{_}") +*>
      Seq( TextInput("input#last", "Gupta"), TextInput("input#last", "Krishnamurthy")) +>
      Submit("input[name=\"search\"]")
      !=!()).join(
        "ol#result-set h2 a"
      )().select (
      "name" -> (_.text1("span.full-name")),
      "title" -> (_.text1("p.title")),
      "skills" -> (_.text("div#profile-skills li"))
    ).asSchemaRDD()
  }
}
