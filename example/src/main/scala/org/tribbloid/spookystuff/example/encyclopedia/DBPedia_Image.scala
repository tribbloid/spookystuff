package org.tribbloid.spookystuff.example.encyclopedia

import org.tribbloid.spookystuff.SpookyContext
import org.tribbloid.spookystuff.actions._
import org.tribbloid.spookystuff.dsl._
import org.tribbloid.spookystuff.example.QueryCore

import scala.concurrent.duration._

/**
 * Created by peng on 14/06/15.
 */
object DBPedia_Image extends QueryCore {

  def imgPages(spooky: SpookyContext, cls: String, str: String) = spooky.fetch(
    Wget(s"http://lookup.dbpedia.org/api/search/KeywordSearch?QueryClass=$cls&QueryString=$str")
  ).wgetJoin(
      S"Result URI".text,
      failSafe = 2
    ).explore(
      S"""a[rel^=dbo][href*=dbpedia],a[rev^=dbo][href*=dbpedia]""".distinctBy(_.href).slice(0,30),
      depthKey = 'depth,
      maxDepth = 2
    )(Try(Delay(2.seconds) +> Wget('A), 3))(
      S"h1#title a".text.replaceAll("http://dbpedia.org.resource/","") ~ 'name
    ).distinctBy('name)
    .fetch(
      Visit("http://images.google.com/")
        +> TextInput("input[name=\"q\"]",'name)
        +> Submit("input[name=\"btnG\"]")
    )

  override def doMain(spooky: SpookyContext) = {

    val str = "Barack Obama"
    val cls = "person"

    val imgs = imgPages(spooky, cls, str)
    imgs.wgetJoin(S"div#search img".src, maxOrdinal = 1)
      .persist()
      .savePages(
        x"file://${System.getProperty("user.home")}/spooky-example/$appName/${str}_$cls/level_${'depth}_${'name.andMap(v =>v.toString.replaceAll("[^\\w]","_"))}"
      ).select(
        S.saved ~ 'path
      ).toDF()
  }
}