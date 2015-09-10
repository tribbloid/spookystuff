package com.tribbloids.spookystuff.example.image

import com.tribbloids.spookystuff.SpookyContext
import com.tribbloids.spookystuff.actions._
import com.tribbloids.spookystuff.dsl._
import com.tribbloids.spookystuff.example.QueryCore

/**
 * Created by peng on 10/06/14.
 */
object GoogleImage_Universities extends QueryCore {

  override def doMain(spooky: SpookyContext) = {

    spooky
      .fetch(
        Wget("http://www.utexas.edu/world/univ/alpha/")
      )
      .join(S"div.box2 a".texts.distinct ~ 'name)(
        Visit("http://images.google.com/")
          +> TextInput("input[name=\"q\"]","Logo '{name}")
          +> Submit("input[name=\"btnG\"]")
      )()
      .wgetJoin(S"div#search img".src, maxOrdinal = 1)
      .persist()
      .savePages(
        x"file://${System.getProperty("user.home")}/spooky-example/$appName/images/${'name}"
      )
      .select(
        S.saved ~ 'path
      )
      .toDF()
  }
}