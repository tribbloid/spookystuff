package org.tribbloid.spookystuff.example.api

import org.tribbloid.spookystuff.actions._
import org.tribbloid.spookystuff.example.QueryCore
import org.tribbloid.spookystuff.expressions.Expression
import org.tribbloid.spookystuff.http.HttpUtils
import org.tribbloid.spookystuff.{SpookyContext, dsl}

import scala.language.postfixOps

/**
 * Created by peng on 29/07/15.
 */
object Yelp_MyMemory_Alchemy extends QueryCore {

  override def doMain(spooky: SpookyContext): Any = {

    import dsl._
    import spooky.dsl._

    val consumerKey = "zfiG0XPsYgSAQ7iSXL6D5g"
    val consumerSecret = "MkMaVzoOL_s-00y0Agd5V9ZAEaU"
    val token = "KV7SgT34ZxJ5n2m5FgiXetdTBgnKOpge"
    val tokenSecret = "xfudMw9Xf3S3GBosQfPI-XY6K8w"

    def sign(url: Expression[String]): Expression[String] = url.andMap(
      HttpUtils.OauthV2(_, consumerKey, consumerSecret, token, tokenSecret)
    )

    val result = sc.parallelize(Seq(Map("q" -> "Epicure", "city" -> "Paris", "lang" -> "fr")))
      .wget(sign("http://api.yelp.com/v2/search?term='{q}&location='{city}"))
      .join((S \ "businesses").slice(0, 10))(
        Try(Wget(sign(x"http://api.yelp.com/v2/business/${'A \ "id" text}?lang=${'lang}")))
      )(
        ('A \ "id").text ~ 'id,
        ('A \ "name").text ~ 'name
      )
      .flatSelect(S \ "reviews")(
        ('A \ "rating" text) ~ 'rating,
        ('A \ "excerpt" text) ~ 'excerpt
      )
      .toDF().persist()

    result
  }
}