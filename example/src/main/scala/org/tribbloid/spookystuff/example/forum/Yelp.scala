package org.tribbloid.spookystuff.example.forum

import org.tribbloid.spookystuff.SpookyContext
import org.tribbloid.spookystuff.actions.Wget
import org.tribbloid.spookystuff.example.ExampleCore
import org.tribbloid.spookystuff.dsl._

/**
 * Created by peng on 9/26/14.
 */
object Yelp extends ExampleCore {

  override def doMain(spooky: SpookyContext) = {
    import spooky._

    sc.parallelize(Seq(
      "http://www.yelp.com/biz/bottega-louie-los-angeles?sort_by=date_desc",
      "http://www.yelp.com/biz/wurstk%C3%BCche-los-angeles-2?sort_by=date_desc",
      "http://www.yelp.com/biz/daikokuya-los-angeles?sort_by=date_desc",
      "http://www.yelp.com/biz/pizzeria-mozza-los-angeles?sort_by=date_desc",
      "http://www.yelp.com/biz/sushi-gen-los-angeles?sort_by=date_desc",
      "http://www.yelp.com/biz/animal-los-angeles?sort_by=date_desc",
      "http://www.yelp.com/biz/blu-jam-caf%C3%A9-los-angeles-2?sort_by=date_desc",
      "http://www.yelp.com/biz/langers-los-angeles-2?sort_by=date_desc",
      "http://www.yelp.com/biz/roscoes-house-of-chicken-and-waffles-los-angeles-3?sort_by=date_desc",
      "http://www.yelp.com/biz/masa-of-echo-park-los-angeles?sort_by=date_desc",
      "http://www.yelp.com/biz/bld-los-angeles?sort_by=date_desc",
      "http://www.yelp.com/biz/providence-los-angeles-2?sort_by=date_desc"
    ))
      .fetch(
        Wget('_)
      )
      .wgetExplore($"a.page-option.prev-next:contains(→)", depthKey = 'page)
      .flatSelect($"div.review", indexKey = 'row) (
        A"p.review_comment".text ~ 'comment,
        A"div.review-content span.rating-qualifier".text ~ 'date_status,
        A"div.biz-rating div div.rating-very-large meta".attr("content") ~ 'stars,
        A"div.review-wrapper > div.review-footer a.ybtn.useful span.i-wrap span.count".text ~ 'useful,
        A"li.user-name a.user-display-name".text ~ 'user_name,
        A"li.user-location".text ~ 'user_location,
        A"li.friend-count b" ~ 'friend_count,
        A"li.review-count b".text ~ 'review_count
      )
      .toSchemaRDD()
  }
}
