package org.tribbloid.spookystuff.example.encyclopedia

import org.tribbloid.spookystuff.SpookyContext
import org.tribbloid.spookystuff.actions._
import org.tribbloid.spookystuff.dsl._
import org.tribbloid.spookystuff.example.QueryCore

/**
 * Created by peng on 14/06/15.
 */
object DBPedia_Collage extends QueryCore {

  override def doMain(spooky: SpookyContext) = {

    val str = "Adam Sandler"
    val cls = "person"

    spooky.fetch(
      Wget(s"http://lookup.dbpedia.org/api/search/KeywordSearch?QueryClass=$cls&QueryString=$str")
    ).wgetJoin(
        S"Result URI".text,
        failSafe = 2
      ).wgetExplore(
        S"""a[rel^=dbpprop][href*="//dbpedia.org"],a[rev^=dbpprop][href*="//dbpedia.org"]""".distinctBy(_.href).slice(0,25),
        failSafe = 2,
        depthKey = 'depth,
        maxDepth = 2
      ).join(S"h1#title a".text, distinct = true)(
        Visit("http://images.google.com/")
          +> TextInput("input[name=\"q\"]",'A)
          +> Submit("input[name=\"btnG\"]")
      )('A ~ 'name).wgetJoin(S"div#search img".src, maxOrdinal = 1)
      .persist()
      .savePages(
        x"file://${System.getProperty("user.home")}/spooky-example/$appName/images/level_${'depth}_${'name}"
      ).select(
        S.saved ~ 'path
      ).toDF()
  }
}