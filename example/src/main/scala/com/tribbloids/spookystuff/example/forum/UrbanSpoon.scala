package com.tribbloids.spookystuff.example.forum

import com.tribbloids.spookystuff.{dsl, SpookyContext}
import com.tribbloids.spookystuff.actions._
import com.tribbloids.spookystuff.example.QueryCore
import dsl._


/**
 * Created by peng on 10/6/14.
 */
object UrbanSpoon extends QueryCore {

  override def doMain(spooky: SpookyContext) = {
    import spooky.dsl._

    sc.parallelize(Seq(
      "http://www.urbanspoon.com/r/5/1435892/restaurant/Downtown/Bottega-Louie-LA",
      "http://www.urbanspoon.com/r/5/778534/restaurant/Downtown/Wurstkuche-LA",
      "http://www.urbanspoon.com/r/5/63832/restaurant/Little-Tokyo/Daikokuya-LA",
      "http://www.urbanspoon.com/r/5/73524/restaurant/West-Hollywood/Pizzeria-Mozza-LA",
      "http://www.urbanspoon.com/r/5/76268/restaurant/Little-Tokyo/Sushi-Gen-LA",
      "http://www.urbanspoon.com/r/5/452223/restaurant/Mid-City-West/Animal-LA",
      "http://www.urbanspoon.com/r/5/61568/restaurant/Mid-City-West/Blu-Jam-Cafe-LA",
      "http://www.urbanspoon.com/r/5/69509/restaurant/Westlake/Langers-Deli-LA",
      "http://www.urbanspoon.com/r/5/74524/restaurant/LA/Roscoes-House-of-Chicken-Waffles-Long-Beach",
      "http://www.urbanspoon.com/r/5/70783/restaurant/Echo-Park/Masa-of-Echo-Park-LA",
      "http://www.urbanspoon.com/r/5/61528/restaurant/Mid-City-West/BLD-LA",
      "http://www.urbanspoon.com/r/5/73788/restaurant/Mid-Wilshire/Providence-LA"
    ),12)
      .flatMap(url => Seq("#reviews","#blog_posts").map(tpe => tpe+"\t"+url+tpe))
      .tsvToMap("type\turl")
      .fetch(
        Visit("'{url}")
          +> Click("ul.PostTabs li.active a")
          +> WaitFor("div.tab-pane.active li.review")
      )
      //      .extract(
      //        "count" -> (_.text1("li.active span.count"))
      //      )
      .flatExtract(S"div.tab-pane.active li.review", ordinalKey = 'row)(
        A"div.body".text ~ 'comment,
        A"time.posted-on".text ~ 'date_status,
        A"div.details > div.aside".text ~ 'stars,
        A"div.title a".text ~ 'user_name,
        A"span.type".text ~ 'user_location,
        A"div.byline a".text ~ 'review_count
      )
      .toDF()
  }
}