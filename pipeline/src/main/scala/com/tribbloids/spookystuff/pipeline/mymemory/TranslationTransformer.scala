package com.tribbloids.spookystuff.pipeline.mymemory

import java.util.UUID

import com.tribbloids.spookystuff.extractors.Extractor
import com.tribbloids.spookystuff.pipeline.RemoteTransformer
import com.tribbloids.spookystuff.rdd.FetchedDataset
import com.tribbloids.spookystuff.{dsl, SpookyContext}

import scala.language.postfixOps

/**
  * Created by peng on 15/11/15.
  */
class TranslationTransformer(
    override val uid: String =
      classOf[TranslationTransformer].getCanonicalName + "_" + UUID.randomUUID().toString
) extends RemoteTransformer {

  import dsl._
  import org.apache.spark.ml.param._

  final val QueryCol: Param[Symbol] = new Param(this, "QueryCol", "Original Text Column")
  final val LangPair: Param[Extractor[Any]] = new Param(
    this,
    "LangPair",
    "Source and language pair, separated by the | symbol. Use ISO standard names or RFC3066")
  final val OutputCol: Param[Symbol] = new Param(this, "OutputCol", "Translated Text Column")

  setExample(QueryCol -> '_, LangPair -> "en|fr", OutputCol -> 'output)

  override def exampleInput(spooky: SpookyContext): FetchedDataset =
    spooky.create(
      Seq(
        Map("_" -> "Hello!")
      ))

  override def transform(dataset: FetchedDataset): FetchedDataset = {

    dataset
      .wget(x"http://api.mymemory.translated.net/get?q=${getOrDefault(QueryCol)}!&langpair=${getOrDefault(LangPair)}")
      .select((S \ "responseData" \ "translatedText" text) ~ 'text)
  }
}
