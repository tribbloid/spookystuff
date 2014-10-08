package org.tribbloid.spookystuff.integration.forum

import org.apache.spark.sql.SchemaRDD
import org.tribbloid.spookystuff.integration.TestCore
import org.tribbloid.spookystuff.entity.client._


/**
 * Created by peng on 10/6/14.
 */
object UrbanSpoon extends TestCore {

  import spooky._

  override def doMain(): SchemaRDD = {
    (sc.parallelize(Seq(
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
      .flatMap(url => Seq("#reviews","#blog_posts").map(tag => tag+"\t"+url+tag))
      .tsvToMap("type\turl")
      +> Visit("#{url}")
      +> Click("ul.PostTabs li.active a")
      +> DelayFor("div.tab-pane.active li.review")
      !=! ())
      //      .extract(
      //        "count" -> (_.text1("li.active span.count"))
      //      )
      .sliceJoin("div.tab-pane.active li.review")(indexKey = "row")
      .extract(
        "comment" -> (_.text1("div.body")),
        "date&status" -> (_.text1("time.posted-on")),
        "stars" -> (_.text1("div.details > div.aside")),
        "user_name" -> (_.text1("div.title a")),
        "user_location" -> (_.text1("span.type")),
        "review_count" -> (_.text1("div.byline a"))
      )
      .asSchemaRDD()
  }
}
