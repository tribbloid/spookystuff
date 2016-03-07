package com.tribbloids.spookystuff.pipeline.mymemory

import java.util.UUID

import com.tribbloids.spookystuff.expressions.Expression
import com.tribbloids.spookystuff.pipeline.RemoteTransformer
import com.tribbloids.spookystuff.execution.AbstractExecutionPlan
import com.tribbloids.spookystuff.{SpookyContext, dsl}

/**
  * Created by peng on 15/11/15.
  */
class TranslationTransformer(
                              override val uid: String =
                              classOf[TranslationTransformer].getCanonicalName + "_" + UUID.randomUUID().toString
                            ) extends RemoteTransformer{


  import dsl._
  import org.apache.spark.ml.param._

  final val QueryCol: Param[Symbol] = new Param(this, "QueryCol", "Original Text Column")
  final val LangPair: Param[Expression[Any]] = new Param(this, "LangPair", "Source and language pair, separated by the | symbol. Use ISO standard names or RFC3066")
  final val OutputCol: Param[Symbol] = new Param(this, "OutputCol", "Translated Text Column")

  setExample(QueryCol -> '_, LangPair -> "en|fr", OutputCol -> 'output)

  override def exampleInput(spooky: SpookyContext): AbstractExecutionPlan = spooky.create(Seq(
    Map("_" -> "Hello!")
  ))

  override def transform(dataset: AbstractExecutionPlan): AbstractExecutionPlan = {

    dataset
      .wget(x"http://api.mymemory.translated.net/get?q=${getOrDefault(QueryCol)}!&langpair=${getOrDefault(LangPair)}")
      .select((S\"responseData"\"translatedText" text) ~ 'text)
  }
}
