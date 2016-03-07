package com.tribbloids.spookystuff.pipeline.alchemyapi

import java.util.UUID

import com.tribbloids.spookystuff.pipeline.RemoteTransformer
import com.tribbloids.spookystuff.rdd.PageRowRDD
import com.tribbloids.spookystuff.{SpookyContext, dsl}

import scala.language.postfixOps

/**
  * Created by peng on 15/11/15.
  */
class GetKeywordsTransformer(
                              override val uid: String =
                              classOf[GetKeywordsTransformer].getCanonicalName + "_" + UUID.randomUUID().toString
                            ) extends RemoteTransformer {

  import dsl._
  import org.apache.spark.ml.param._

  final val APIKey: Param[String] = new Param(this, "APIKey", "Alchemi API Key")
  final val InputCol: Param[Symbol] = new Param(this, "InputCol", "Text being analyzed")
  final val StrictExtractionOnly: Param[Boolean] = new Param(this, "StrictExtractionOnly", "if set to true, only extract strict keywords")
  final val KeywordCol: Param[Symbol] = new Param(this, "KeywordCol", "detected keywords")
  final val RelevanceCol: Param[Symbol] = new Param(this, "RelevanceCol", "relevance of detected keywords")
  final val Sentiment: Param[Boolean] = new Param(this, "Sentiment", "set to true to enable sentimental analysis of each keyword")
  final val SentimentCol: Param[Symbol] = new Param(this, "SentimentCol", "sentiment of detected keywords")

  //  final val KnowledgeGraph: Param[Boolean] = new Param(this, "KnowledgeGraph", "set to true to enable knowledge graph") TODO: enable it!

  setDefault(StrictExtractionOnly -> false, Sentiment -> false)
  setExample(
    APIKey -> "500796b5ea023b5db04d48ca70b6d4804c83a9d5",
    InputCol -> '_,
    KeywordCol -> 'keyword,
    RelevanceCol -> 'relevance,
    Sentiment -> true,
    SentimentCol -> 'sentiment
  )

  override def exampleInput(spooky: SpookyContext): PageRowRDD = spooky.create(Seq(
    Map("_" -> "AlchemyAPI has raised $2 million to extend the capabilities of its deep learning technology that applies artificial intelligence to read and understand web pages, text documents, emails, tweets, and other forms of content. Access Venture Partners led the Series A round, which the company will use to ramp up its sales and marketing, make hires and launch new services.")
  ))

  override def transform(dataset: PageRowRDD): PageRowRDD = {
    val mode: String = if (getOrDefault(StrictExtractionOnly)) "strict"
    else "normal"

    val sentiment: Int = if (getOrDefault(Sentiment)) 1
    else 0

    dataset
      .wget(
        x"http://access.alchemyapi.com/calls/text/TextGetRankedKeywords?apikey=${getOrDefault(APIKey)}&text=${getOrDefault(InputCol)}" +
          s"&keywordExtractMode=$mode&sentiment=$sentiment&outputMode=json&knowledgeGraph=0"
      )
      .flatExtract(S \ "keywords")(
        ('A \ "text" text) ~ getOrDefault(KeywordCol),
        ('A \ "relevance" text) ~ getOrDefault(RelevanceCol),
        ('A \ "sentiment" \ "score" text) ~ getOrDefault(SentimentCol)
      )
  }
}
