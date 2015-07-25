package org.tribbloid.spookystuff.example.encyclopedia

import org.tribbloid.spookystuff.SpookyContext
import org.tribbloid.spookystuff.actions._
import org.tribbloid.spookystuff.dsl._
import org.tribbloid.spookystuff.example.QueryCore

/**
 * Created by peng on 14/06/15.
 */
object DBPedia_Film extends QueryCore {

  override def doMain(spooky: SpookyContext) = {

    import spooky.dsl._

    import sql.implicits._

    sc.parallelize(
      "Gladiator".split(",").map(_.trim)
    ).fetch(
        Wget("http://lookup.dbpedia.org/api/search/KeywordSearch?QueryClass=film&QueryString='{_}")
      ).join(S"Result".slice(0,3))(
        Try(Wget(A"URI".text),2)
      )(
        A"Label".text ~ 'movie
      ).join(S"a[rel^=dbpedia-owl][href*=dbpedia]".distinctBy(_.href))(
        Try(Wget('A.href),2)
      )(
        'A.text.replaceAll("dbpedia:", "") ~ 'personnel
      ).flatSelect(S"a[rev*=guests]", left=false)(
        'A.text.replaceAll("dbpedia:List_of_", "").replaceAll("_episodes.*","") ~ 'guest_of,
        'A.text.replaceAll("dbpedia:List_of_", "").replaceAll(".*episodes_","") ~ 'episodes
      ).toDF(sort = true)
  }
}