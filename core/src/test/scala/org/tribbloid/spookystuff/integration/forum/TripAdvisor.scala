package org.tribbloid.spookystuff.integration.forum

import org.tribbloid.spookystuff.actions._
import org.tribbloid.spookystuff.integration.TestCore

/**
 * Created by peng on 9/26/14.
 */
object TripAdvisor extends TestCore {

  import spooky._

  def doMain() = {

    (sc.parallelize(Seq(
      "http://www.tripadvisor.ca/Restaurant_Review-g32655-d1474645-Reviews-Bottega_Louie-Los_Angeles_California.html#REVIEWS"
//      "http://www.tripadvisor.ca/Restaurant_Review-g32655-d1380148-Reviews-Wurstkuche-Los_Angeles_California.html",
//      "http://www.tripadvisor.ca/Restaurant_Review-g32655-d1022582-Reviews-Daikokuya-Los_Angeles_California.html",
//      "http://www.tripadvisor.ca/Restaurant_Review-g32655-d782715-Reviews-Pizzeria_Mozza-Los_Angeles_California.html",
//      "http://www.tripadvisor.ca/Restaurant_Review-g32655-d364705-Reviews-Sushi_Gen-Los_Angeles_California.html",
//      "http://www.tripadvisor.ca/Restaurant_Review-g32655-d1464163-Reviews-Animal-Los_Angeles_California.html",
//      "http://www.tripadvisor.ca/Restaurant_Review-g32655-d4607221-Reviews-Blu_Jam_Cafe-Los_Angeles_California.html",
//      "http://www.tripadvisor.ca/Restaurant_Review-g32655-d364756-Reviews-Langer_s-Los_Angeles_California.html",
//      "http://www.tripadvisor.ca/Restaurant_Review-g32655-d364727-Reviews-Roscoe_s_House_of_Chicken_Waffles-Los_Angeles_California.html",
//      "http://www.tripadvisor.ca/Restaurant_Review-g32655-d814338-Reviews-Masa_of_Echo_Park-Los_Angeles_California.html",
//      "http://www.tripadvisor.ca/Restaurant_Review-g32655-d878142-Reviews-BLD-Los_Angeles_California.html",
//      "http://www.tripadvisor.ca/Restaurant_Review-g32655-d594024-Reviews-Providence-Los_Angeles_California.html"
    ),12)
      +> Visit("#{_}")
      +> Try(Click("span.partnerRvw span.taLnk") :: Nil)
      !=!())
      .paginate("a.sprite-pageNext", wget = false, postAction = Try(Click("span.partnerRvw span.taLnk")::Nil) :: Nil)(indexKey = "page")
      .sliceJoin("div.reviewSelector")(indexKey = "row")
      .extract(
        "comment" -> (_.text1("p", last=true)),
        "date&status" -> (_.text1("span.ratingDate", last=true)),
        "stars" -> (_.attr1("div.innerBubble img.sprite-rating_s_fill","alt", last=true)),
        "useful" -> (_.text1("span.numHlpIn", last=true)),
        "user_name" -> (_.text1("div.member_info div.username mo", last=true)),
        "user_location" -> (_.text1("div.member_info div.location", last=true)),
        "city_count" -> (_.text1("div.passportStampsBadge span.badgeText", last=true)),
        "review_count" -> (_.text1("div.totalReviewBadge", last=true))
      )
      .asSchemaRDD()
  }
}
