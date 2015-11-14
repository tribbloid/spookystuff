//package com.tribbloids.spookystuff.pipeline.dbpedia
//
//import java.util.UUID
//
//import com.tribbloids.spookystuff.actions.Wget
//import com.tribbloids.spookystuff.expressions.Expression
//import com.tribbloids.spookystuff.pipeline.SpookyTransformer
//import com.tribbloids.spookystuff.sparkbinding.PageRowRDD
//import com.tribbloids.spookystuff.{SpookyContext, dsl}
//
///**
//  * Created by peng on 31/10/15.
//  */
//class RelationTransformer(
//                          override val uid: String =
//                          classOf[RelationTransformer].getCanonicalName + "_" + UUID.randomUUID().toString
//                          ) extends SpookyTransformer {
//
//                             import dsl._
//                             import org.apache.spark.ml.param._
//
//                             final val InputCol: Param[Symbol] = new Param[Symbol](this, "QueryCol", "query column name")
//                             final val relation: Param[String] = new Param[String](rel, "firstN", "only take the first N responses")
//                             final val reverseRelation: Param[String] = new Param[String](rev, "IndexCol", "index of output")
//                             final val LabelCol: Param[Symbol] = new Param[Symbol](this, "LabelCol", "output Entity column name")
//                             final val UriCol: Param[Symbol] = new Param[Symbol](this, "UriCol", "output Uri column name")
//
//                             setExample(InputCol -> '_, firstN -> 3, IndexCol -> 'index,LabelCol -> 'label, UriCol -> 'uri)
//                             setDefault(firstN -> Int.MaxValue, LabelCol -> null, UriCol -> null)
//
//                             override def exampleInput(spooky: SpookyContext): PageRowRDD = spooky.create(Seq(
//                               Map("q" ->"Barack Obama", "class" -> "person")
//                             ))
//
//                             override def transform(dataset: PageRowRDD): PageRowRDD = {
//
//                               var uri: Expression[String] = x"http://lookup.dbpedia.org/api/search/KeywordSearch?QueryString=${getOrDefault(QueryCol)}"
//                               if (getOrDefault(ClassCol) != null) uri = uri + x"&QueryClass=${getOrDefault(ClassCol)}"
//
//                               dataset.fetch(
//                                 Wget(uri)
//                               ).flatSelect(S"Result".slice(0, getOrDefault(firstN)), ordinalKey = getOrDefault(IndexCol))(
//                                 A"Label".text ~ getOrDefault(LabelCol),
//                                 A"URI".text ~ getOrDefault(UriCol)
//                               )
//                             }
//                           }