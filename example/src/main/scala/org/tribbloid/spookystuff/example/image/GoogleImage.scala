package org.tribbloid.spookystuff.example.image

import org.tribbloid.spookystuff.SpookyContext
import org.tribbloid.spookystuff.actions._
import org.tribbloid.spookystuff.dsl._
import org.tribbloid.spookystuff.example.ExampleCore

/**
 * Created by peng on 10/06/14.
 */
object GoogleImage extends ExampleCore {

  override def doMain(spooky: SpookyContext) = {
    import spooky._

    noInput
      .fetch(
        Visit("http://www.utexas.edu/world/univ/alpha/")
      )
      .flatten($"div.box2 a".text ~ 'name, limit = 10)
      .repartition(10)
      .fetch(
        Visit("http://images.google.com/")
          +> WaitFor("form[action=\"/search\"]")
          +> TextInput("input[name=\"q\"]","Logo '{name}")
          +> Submit("input[name=\"btnG\"]")
          +> WaitFor("div#search")
      )
      .wgetJoin($"div#search img".src, limit = 1)
      .save(
        x"file://${System.getProperty("user.home")}/spooky-example/$appName/images/${'name}"
      )
      .select(
        $.saved ~ 'path
      )
      .asSchemaRDD()
  }
}