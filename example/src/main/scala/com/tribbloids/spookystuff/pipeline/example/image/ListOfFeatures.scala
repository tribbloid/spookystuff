package com.tribbloids.spookystuff.pipeline.example.image

import com.tribbloids.spookystuff.SpookyContext
import com.tribbloids.spookystuff.dsl._
import com.tribbloids.spookystuff.example.QueryCore
import com.tribbloids.spookystuff.pipeline.transformer.google.GoogleImageTransformer
import org.apache.spark.rdd.RDD

/**
 * Created by peng on 10/06/14.
 */
object ListOfFeatures extends QueryCore {

  case class Features(
                       description: String,
                       list: String
                       )

  override def doMain(spooky: SpookyContext) = {

    import spooky.dsl._
    import spooky.sqlContext.implicits._

    val input = sc.parallelize(Seq(
      Features("Supported Browsers", "PhantomJS, HtmlUnit"),
      Features("Browser Actions", "Visit, Click, Drop Down Select, Text Input"),
      Features("Query Optimizers", "Narrow, Wide")
    )).toDF()
    .explode[String, String]("list","item")(_.split(",").map(v => "logo "+v.trim))

    val m = new GoogleImageTransformer().setInputCol("item").setImageUrisCol("uris")
      .transform(input)
      .select('uris.head ~ 'logo)
      .toPairRDD('description, 'logo)

    val output: RDD[(String, String)] = new GoogleImageTransformer().setInputCol("item").setImageUrisCol("uris")
      .transform(input)
      .select(x"""<img src="${'uris.head}"/>""" ~ 'logo)
      .toPairRDD('description, 'logo)
      .groupByKey()
      .map(tuple => tuple._1.asInstanceOf[String] -> ("%html " + tuple._2.mkString))

    output.toDF()
  }
}