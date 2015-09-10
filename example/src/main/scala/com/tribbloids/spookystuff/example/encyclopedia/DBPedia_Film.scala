package com.tribbloids.spookystuff.example.encyclopedia

import com.tribbloids.spookystuff.SpookyContext
import com.tribbloids.spookystuff.actions._
import com.tribbloids.spookystuff.dsl._
import com.tribbloids.spookystuff.example.QueryCore

/**
 * Created by peng on 14/06/15.
 */
object DBPedia_Film extends QueryCore {

  override def doMain(spooky: SpookyContext) = {

    import spooky.dsl._

    import sql.implicits._

    sc.parallelize(
      "Jurassic Park".split(",").map(_.trim)
    ).fetch(
        Wget("http://lookup.dbpedia.org/api/search/KeywordSearch?QueryClass=film&QueryString='{_}")
      ).join(S"Result".slice(0,3))(
        Try(Wget(A"URI".text),2)
      )(
        A"Label".text ~ 'movie
      ).join(S"a[rel^=dbo][href*=dbpedia]".distinctBy(_.href))(
        Try(Wget('A.href),2)
      )(
        S.uri ~ 'uri,
        'A.text.replaceAll("dbpedia:", "") ~ 'personnel
      ).flatSelect(S"a[rev*=guests]:contains(dbpedia:List_of_)")(
        'A.text.replaceAll("dbpedia:List_of_", "").replaceAll("_episodes.*","") ~ 'guest_of,
        'A.text.replaceAll("dbpedia:List_of_", "").replaceAll(".*episodes_","") ~ 'episodes
      ).toDF(sort = true)
  }
}