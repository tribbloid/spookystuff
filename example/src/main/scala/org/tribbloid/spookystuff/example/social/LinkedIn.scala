package org.tribbloid.spookystuff.example.social

import org.tribbloid.spookystuff.SpookyContext
import org.tribbloid.spookystuff.actions._
import org.tribbloid.spookystuff.dsl._
import org.tribbloid.spookystuff.example.ExampleCore

/**
 * A more complex linkedIn job that finds name and printout skills of all Sanjay Gupta in your local area
 */
//remember infix operator cannot be written in new line
object LinkedIn extends ExampleCore {

  override def doMain(spooky: SpookyContext) = {
    import spooky._

    sc.parallelize(Seq("Sanjay", "Arun", "Hardik"))
      .fetch(
        Visit("https://www.linkedin.com/")
          +> TextInput("input#first", '_)
          *> (TextInput("input#last", "Gupta") :: TextInput("input#last", "Krishnamurthy") :: Nil)
          +> Submit("input[name=\"search\"]")
      )
      .visitJoin($"ol#result-set h2 a")
      .select(
        $"span.full-name".text ~ 'name,
        $"p.title".text ~ 'title,
        $"div#profile-skills li".texts.mkString("|") ~ 'skills,
        $.uri as 'uri
      )
      .asSchemaRDD()
  }
}
