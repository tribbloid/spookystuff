package org.tribbloid.spookystuff.integration.forum

import org.tribbloid.spookystuff.actions._
import org.tribbloid.spookystuff.integration.TestCore

/**
 * Created by peng on 9/26/14.
 */
object GooglePlusReview extends TestCore {

  import spooky._

  def doMain() = {

    (sc.parallelize(Seq(
    "https://plus.google.com/111847129364038020241/about",
    "https://plus.google.com/106558611403361134160/about",
    "https://plus.google.com/104711146492211746858/about",
    "https://plus.google.com/+PizzeriaMozzaLosAngeles/about",
    "https://plus.google.com/100930306416993024046/about",
    "https://plus.google.com/100784476574378352054/about",
    "https://plus.google.com/107378460863753921248/about",
    "https://plus.google.com/+LangersDelicatessenRestaurant/about",
    "https://plus.google.com/109122106234152764867/about",
    "https://plus.google.com/109825052255681007824/about",
    "https://plus.google.com/109302266591770935966/about",
    "https://plus.google.com/108890902290663191606/about"
    ),12)
      +> Visit("#{_}")
      +> Loop(Click("div.R4 span.d-s") :: Nil)
      !=!())
      .sliceJoin("div.Qxb div.Ee")(indexKey = "row")
      .extract(
        "comment" -> (_.text1("div.VSb span.GKa")),
        "date&status" -> (_.text1("span.VUb")),
        "stars" -> (_.numElements("div.b-db span.b-db-ac-th")),
        "user_name" -> (_.text1("span.Gl a.d-s"))
//        "user_location" -> (_ => "pending"),
//        "friend_count" -> (_ => "pending"),
//        "review_count" -> (_ => "pending")
      )
      .asSchemaRDD()
  }
}
