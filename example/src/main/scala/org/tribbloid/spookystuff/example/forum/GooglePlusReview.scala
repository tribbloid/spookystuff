package org.tribbloid.spookystuff.example.forum

import org.tribbloid.spookystuff.{dsl, SpookyContext}
import org.tribbloid.spookystuff.actions._
import org.tribbloid.spookystuff.example.ExampleCore
import dsl._

/**
 * Created by peng on 9/26/14.
 */
object GooglePlusReview extends ExampleCore {

  override def doMain(spooky: SpookyContext) = {
    import spooky._

    sc.parallelize(Seq(
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
      .fetch(
        Visit("'{_}")
          +> LoadMore("div.R4 span.d-s")
      )
      .flatSelect($("div.Qxb div.Ee"), ordinalKey = 'row)(
        A("div.VSb span.GKa").text ~ 'comment,
        A("span.VUb").text ~ 'date_status,
        A("div.b-db span.b-db-ac-th").size ~ 'stars,
        A("span.Gl a.d-s").text ~ 'user_name
      )
      .toSchemaRDD()
  }
}
