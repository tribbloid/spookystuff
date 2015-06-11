package org.tribbloid.spookystuff.example.image

import org.tribbloid.spookystuff.SpookyContext
import org.tribbloid.spookystuff.actions._
import org.tribbloid.spookystuff.dsl._
import org.tribbloid.spookystuff.example.QueryCore

/**
 * Created by peng on 10/06/14.
 */
object GoogleImage extends QueryCore {

  override def doMain(spooky: SpookyContext) = {

    spooky
      .fetch(
        Visit("http://www.utexas.edu/world/univ/alpha/")
      )
      .flatten($"div.box2 a".texts ~ 'name)
      .repartition(10)
      .fetch(
        Visit("http://images.google.com/")
          +> WaitFor("form[action=\"/search\"]")
          +> TextInput("input[name=\"q\"]","Logo '{name}")
          +> Submit("input[name=\"btnG\"]")
          +> WaitFor("div#search")
      )
      .wgetJoin($"div#search img".src, maxOrdinal = 1)
      .persist()
      .savePages(
        x"file://${System.getProperty("user.home")}/spooky-example/$appName/images/${'name}"
      )
      .select(
        $.saved ~ 'path
      )
      .toDF()
  }
}