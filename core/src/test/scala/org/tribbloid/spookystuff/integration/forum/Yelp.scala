package org.tribbloid.spookystuff.integration.forum

import org.tribbloid.spookystuff.entity.client.Wget
import org.tribbloid.spookystuff.integration.TestCore

/**
 * Created by peng on 9/26/14.
 */
object Yelp extends TestCore {

  import spooky._

  def doMain() = {

    (sc.parallelize(Seq(
//    "http://www.yelp.com/biz/bottega-louie-los-angeles?sort_by=date_desc",
//    "http://www.yelp.com/biz/wurstk%C3%BCche-los-angeles-2?sort_by=date_desc",
//    "http://www.yelp.com/biz/daikokuya-los-angeles?sort_by=date_desc",
//    "http://www.yelp.com/biz/pizzeria-mozza-los-angeles?sort_by=date_desc",
//    "http://www.yelp.com/biz/sushi-gen-los-angeles?sort_by=date_desc",
//    "http://www.yelp.com/biz/animal-los-angeles?sort_by=date_desc",
//    "http://www.yelp.com/biz/blu-jam-caf%C3%A9-los-angeles-2?sort_by=date_desc",
//    "http://www.yelp.com/biz/langers-los-angeles-2?sort_by=date_desc",
//    "http://www.yelp.com/biz/roscoes-house-of-chicken-and-waffles-los-angeles-3?sort_by=date_desc",
//    "http://www.yelp.com/biz/masa-of-echo-park-los-angeles?sort_by=date_desc",
//    "http://www.yelp.com/biz/bld-los-angeles?sort_by=date_desc",
    "http://www.yelp.com/biz/providence-los-angeles-2?sort_by=date_desc"
    ))
      +> Wget("#{_}")
      !=!())
      .paginate("a.page-option.prev-next:contains(→)")(indexKey = "page", last = true)
      .sliceJoin("div.review")(indexKey = "row")
      .extract(
        "comment" -> (_.text1("p.review_comment")),
        "date&status" -> (_.text1("div.review-content span.rating-qualifier")),
        "stars" -> (_.attr1("div.biz-rating div div.rating-very-large meta","content")),
        "useful" -> (_.text1("div.review-wrapper > div.review-footer a.ybtn.useful span.i-wrap span.count")),
        "user_name" -> (_.text1("li.user-name a.user-display-name")),
        "user_location" -> (_.text1("li.user-location")),
        "friend_count" -> (_.text1("li.friend-count b")),
        "review_count" -> (_.text1("li.review-count b"))
      )
      .asSchemaRDD()
  }
}
