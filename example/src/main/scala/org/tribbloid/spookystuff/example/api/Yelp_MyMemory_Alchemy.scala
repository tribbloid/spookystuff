package org.tribbloid.spookystuff.example.api

import org.apache.tika.language.LanguageIdentifier
import org.tribbloid.spookystuff.actions._
import org.tribbloid.spookystuff.example.QueryCore
import org.tribbloid.spookystuff.expressions.Expression
import org.tribbloid.spookystuff.session.OAuthKeys
import org.tribbloid.spookystuff.{SpookyContext, dsl}

import scala.language.postfixOps

/**
 * Created by peng on 29/07/15.
 */
object Yelp_MyMemory_Alchemy extends QueryCore {

  import dsl._

  def nonEnglish(src: Expression[Any]): Expression[String] = src.andFlatMap{
    str =>
      val identifier = new LanguageIdentifier(str.toString)
      if (identifier.getLanguage == "en") None
      else Some(str.toString)
  }

  def annotate(src: String, word: String, relevance: Double, sentiment: Double): String = {

    val color = 0x00ff*(sentiment + 1)/2 + 0xff00*(1 - sentiment)/2
    val colorStr = "#" + String.format("%04X", color.toInt: Integer) +"00"
    val sizeStr = (relevance*200).toString + "%"

    val regex = word.r
    assert (regex.findAllMatchIn(src).nonEmpty)

    src.replaceAll(word, s"""<span style="color:$colorStr;font-size:$sizeStr">$word</span>""")
  }

  override def doMain(spooky: SpookyContext): Any = {

    import spooky.dsl._
    import sql.implicits._

    spooky.conf.oAuthKeys = () => OAuthKeys(
      "zfiG0XPsYgSAQ7iSXL6D5g",
      "MkMaVzoOL_s-00y0Agd5V9ZAEaU",
      "KV7SgT34ZxJ5n2m5FgiXetdTBgnKOpge",
      "xfudMw9Xf3S3GBosQfPI-XY6K8w"
    )

    val email = "pc175@uow.edu.au"
    val alchemyKey = "500796b5ea023b5db04d48ca70b6d4804c83a9d5"

    sc.parallelize(Seq(Map("q" -> "Epicure", "city" -> "Paris", "lang" -> "fr")))
      .fetch(
        OAuthSign(Wget("http://api.yelp.com/v2/search?term='{q}&location='{city}"))
      )
      .join((S \ "businesses").slice(0, 2))(
        Try(OAuthSign(Wget(x"http://api.yelp.com/v2/business/${'A \ "id" text}?lang=${'lang}")))
      )(
        //        ('A \ "id").text ~ 'id,
        ('A \ "name").text ~ 'name
      )
      .select(
        x"""%html <img src="${S \ "image_url" text}"/>""".orElse("") ~ 'image,
        x"""%html <img src="https://maps.googleapis.com/maps/api/staticmap?size=600x300&maptype=roadmap&markers=color:orange%7C${S \ "location" \ "coordinate" \ "latitude" text},${S \ "location" \ "coordinate" \ "longitude" text}"/>""" ~ 'map
      )
      .flatSelect(S.\("reviews").slice(0, 2))(
        ('A \ "rating" text) ~ 'rating,
        ('A \ "excerpt" text).replaceAll("\n", "") ~ 'excerpt
      )
      .wget(
        x"http://api.mymemory.translated.net/get?q=${nonEnglish('excerpt)}!&langpair=${'lang}|en&de=$email"
      ).select(
        (S \ "responseData" \ "translatedText" text).orElse('excerpt) ~ 'translated
      )
      .wget(
        x"http://access.alchemyapi.com/calls/text/TextGetRankedKeywords?apikey=$alchemyKey&text=${'translated}" +
          "&keywordExtractMode=normal&sentiment=true&outputMode=json&knowledgeGraph=false"
      ).select(
        (S \ "keywords" -> 'translated).andMap{
          tuple =>
            "%html " + tuple._1.foldLeft(tuple._2.toString){
              (str, e) =>
                annotate(str, (e \ "text").text.get, (e \ "relevance").text.get.toDouble, (e \ "sentiment" \ "score").text.get.toDouble)
            }
        } ~ 'annotated
      ).remove('translated)
      .toDF()
  }
}