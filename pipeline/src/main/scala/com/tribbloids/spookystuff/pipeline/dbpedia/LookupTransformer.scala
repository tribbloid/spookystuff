package com.tribbloids.spookystuff.pipeline.dbpedia

import java.util.UUID

import com.tribbloids.spookystuff.actions.Wget
import com.tribbloids.spookystuff.extractors.Extractor
import com.tribbloids.spookystuff.pipeline.RemoteTransformer
import com.tribbloids.spookystuff.rdd.FetchedDataset
import com.tribbloids.spookystuff.{dsl, SpookyContext}

/**
  * Created by peng on 31/10/15.
  */
class LookupTransformer(
    override val uid: String =
      classOf[LookupTransformer].getCanonicalName + "_" + UUID.randomUUID().toString
) extends RemoteTransformer {

  import dsl._
  import org.apache.spark.ml.param._

  final val QueryCol: Param[Symbol] = new Param[Symbol](this, "QueryCol", "query column name")
  final val ClassCol: Param[Symbol] = new Param[Symbol](this, "ClassCol", "class column name")
  final val FirstN: Param[Int] = new Param[Int](this, "FirstN", "only take the first N responses")
  final val IndexCol: Param[Symbol] = new Param[Symbol](this, "IndexCol", "index of output")
  final val LabelCol: Param[Symbol] = new Param[Symbol](this, "LabelCol", "output Entity column name")
  final val UriCol: Param[Symbol] = new Param[Symbol](this, "UriCol", "output Uri column name")

  setExample(QueryCol -> 'q, ClassCol -> 'class, FirstN -> 3, IndexCol -> 'index, LabelCol -> 'label, UriCol -> 'uri)
  setDefault(FirstN -> Int.MaxValue, LabelCol -> null, UriCol -> null)

  override def exampleInput(spooky: SpookyContext): FetchedDataset =
    spooky.create(
      Seq(
        Map("q" -> "Barack Obama", "class" -> "person")
      ))

  override def transform(dataset: FetchedDataset): FetchedDataset = {

    var uri: Extractor[String] =
      x"http://lookup.dbpedia.org/api/search/KeywordSearch?QueryString=${getOrDefault(QueryCol)}"
    if (getOrDefault(ClassCol) != null) uri = uri + x"&QueryClass=${getOrDefault(ClassCol)}"

    dataset
      .fetch(
        Wget(uri)
      )
      .flatSelect(S"Result".slice(0, getOrDefault(FirstN)), ordinalField = getOrDefault(IndexCol))(
        A"Label".text ~ getOrDefault(LabelCol),
        A"URI".text ~ getOrDefault(UriCol)
      )
  }
}
