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
        Wget(A"URI".text)
      )(
        A"Label".text ~ 'fullname
      ).join(S"a[rel*=starring]".distinctBy(_.href))(
        Wget('A.href)
      )(
        'A.text.replaceAll("dbpedia:", "") ~ 'cast
      ).flatSelect(S"a[rev*=guests]", left=true)(
        'A.text.replaceAll("dbpedia:List_of_", "").replaceAll("_episodes.*","") ~ 'guest_of,
        'A.text.replaceAll("dbpedia:List_of_", "").replaceAll(".*episodes_","") ~ 'episodes
      ).toDF(sort = true)
  }
}