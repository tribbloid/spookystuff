package org.tribbloid.spookystuff.example.forum

import org.tribbloid.spookystuff.SpookyContext
import org.tribbloid.spookystuff.actions._
import org.tribbloid.spookystuff.example.QueryCore
import org.tribbloid.spookystuff.dsl._

/**
 * Created by peng on 9/26/14.
 */
object TripAdvisor extends QueryCore {

  override def doMain(spooky: SpookyContext) = {
    import spooky.dsl._

    sc.parallelize(Seq(
      "http://www.tripadvisor.ca/Restaurant_Review-g32655-d1474645-Reviews-Bottega_Louie-Los_Angeles_California.html#REVIEWS",
      "http://www.tripadvisor.ca/Restaurant_Review-g32655-d1380148-Reviews-Wurstkuche-Los_Angeles_California.html",
      "http://www.tripadvisor.ca/Restaurant_Review-g32655-d1022582-Reviews-Daikokuya-Los_Angeles_California.html",
      "http://www.tripadvisor.ca/Restaurant_Review-g32655-d782715-Reviews-Pizzeria_Mozza-Los_Angeles_California.html",
      "http://www.tripadvisor.ca/Restaurant_Review-g32655-d364705-Reviews-Sushi_Gen-Los_Angeles_California.html",
      "http://www.tripadvisor.ca/Restaurant_Review-g32655-d1464163-Reviews-Animal-Los_Angeles_California.html",
      "http://www.tripadvisor.ca/Restaurant_Review-g32655-d4607221-Reviews-Blu_Jam_Cafe-Los_Angeles_California.html",
      "http://www.tripadvisor.ca/Restaurant_Review-g32655-d364756-Reviews-Langer_s-Los_Angeles_California.html",
      "http://www.tripadvisor.ca/Restaurant_Review-g32655-d364727-Reviews-Roscoe_s_House_of_Chicken_Waffles-Los_Angeles_California.html",
      "http://www.tripadvisor.ca/Restaurant_Review-g32655-d814338-Reviews-Masa_of_Echo_Park-Los_Angeles_California.html",
      "http://www.tripadvisor.ca/Restaurant_Review-g32655-d878142-Reviews-BLD-Los_Angeles_California.html",
      "http://www.tripadvisor.ca/Restaurant_Review-g32655-d594024-Reviews-Providence-Los_Angeles_California.html"
    ))
      .fetch(
        Visit('_)
          +> Try(Click("span.partnerRvw span.taLnk") :: Nil)
      )
      .explore(S"a.sprite-pageNext", depthKey = 'page)(
        Visit('A.href)
          +> Try(Click("span.partnerRvw span.taLnk")::Nil)
      )()
      .flatSelect(S"div.reviewSelector", ordinalKey = 'row)(
        A"p".last.text ~ 'comment,
        A"span.ratingDate".last.text ~ 'date_status,
        A"div.innerBubble img.sprite-rating_s_fill".last.attr("alt") ~ 'stars,
        A"span.numHlpIn".last.text ~ 'useful,
        A"div.member_info div.username mo".last.text ~ 'user_name,
        A"div.member_info div.location".last.text ~ 'user_location,
        A"div.passportStampsBadge span.badgeText".last.text ~ 'city_count,
        A"div.totalReviewBadge" ~ 'review_count
      )
      .toDF()
  }
}
