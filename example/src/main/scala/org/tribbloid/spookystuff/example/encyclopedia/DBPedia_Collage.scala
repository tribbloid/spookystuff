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

    import spooky.sqlContext.implicits._

    val str = "Gladiator"
    val cls = "film"

    spooky.fetch(
      Wget(s"http://lookup.dbpedia.org/api/search/KeywordSearch?QueryClass=$cls&QueryString=$str")
    ).join(S"Result".slice(0,1))(
        Wget(A"URI".text)
      )(
//        A"Label".text ~ 'entity,
        A"URI".text ~ 'uri
      ).wgetExplore(
        S"""a[rel^=dbpprop][href*="//dbpedia.org"]""".distinctBy(_.href).slice(0,10),
        failSafe = 2,
        depthKey = 'depth,
        maxDepth = 2,
//        select = 'A.text.replaceAll("dbpedia:","") ~! 'entity
        select = 'A.href ~! 'uri
      ).toDF().orderBy('depth.asc)
  }
}