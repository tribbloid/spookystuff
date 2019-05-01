package com.tribbloids.spookystuff.pipeline.dbpedia

import java.util.UUID

import com.tribbloids.spookystuff.pipeline.RemoteTransformer
import com.tribbloids.spookystuff.rdd.FetchedDataset
import com.tribbloids.spookystuff.{dsl, SpookyContext}

/**
  * Created by peng on 31/10/15.
  */
class RelationTransformer(
    override val uid: String =
      classOf[RelationTransformer].getCanonicalName + "_" + UUID.randomUUID().toString
) extends RemoteTransformer {

  import dsl._
  import org.apache.spark.ml.param._

  final val InputURICol: Param[Symbol] = new Param(this, "InputURICol", "column that contains URI of entities")
  final val Relation: Param[String] = new Param(this, "Relations", "regex of link relations")
  final val ReverseRelation: Param[String] = new Param(this, "ReverseRelations", "regex of reverse link relations")
  final val FirstN: Param[Int] = new Param(this, "FirstN", "only take the first N responses")
  final val depth: Param[Range] = new Param(this, "MaxDepth", "depth of traversing")
  final val DepthCol: Param[Symbol] = new Param(this, "DepthKey", "column that contains Depth of relations")
  final val OutputURICol: Param[Symbol] =
    new Param[Symbol](this, "OutputURICol", "column that contains URI of output entities")

  setExample(InputURICol -> '_,
             Relation -> ".*appointer",
             ReverseRelation -> ".*appointer",
             depth -> (0 to 2),
             DepthCol -> 'depth,
             OutputURICol -> 'uri,
             FirstN -> 3)
  setDefault(depth -> (0 to 1), DepthCol -> null, OutputURICol -> null, FirstN -> Int.MaxValue)

  override def exampleInput(spooky: SpookyContext): FetchedDataset =
    spooky.create(
      Seq(
        Map("_" -> "http://dbpedia.org/page/Barack_Obama")
      ))

  override def transform(dataset: FetchedDataset): FetchedDataset = {

    val r = dataset
      .wget(
        getOrDefault(InputURICol)
      )

    r.wgetExplore(
      S"a"
        .distinctBy(_.href)
        .filter { v =>
          val hrefMatch = v.href.exists(_.contains("://dbpedia"))
          val relMatch = v.attr("rel").exists(_.matches(getOrDefault(Relation)))
          val revMatch = v.attr("rev").exists(_.matches(getOrDefault(ReverseRelation)))
          hrefMatch && (relMatch || revMatch)
        }
        .slice(0, getOrDefault(FirstN)),
      range = getOrDefault(depth),
      depthField = getOrDefault(DepthCol),
      failSafe = 2,
      select = S.uri ~ getOrDefault(OutputURICol)
    )
  }
}
