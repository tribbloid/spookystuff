package org.tribbloid.spookystuff.integration.forum

import org.tribbloid.spookystuff.entity.client.Wget
import org.tribbloid.spookystuff.integration.TestCore

/**
 * Created by peng on 9/26/14.
 */
object Yelp extends TestCore {

  import spooky._

  def doMain() = {

    (noInput
      +> Wget("http://www.yelp.ca/biz/black-hoof-toronto?sort_by=date_desc")
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
