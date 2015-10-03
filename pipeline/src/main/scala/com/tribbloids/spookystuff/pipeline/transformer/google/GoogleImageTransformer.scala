package com.tribbloids.spookystuff.pipeline.transformer.google

import java.util.UUID

import com.tribbloids.spookystuff.actions._
import com.tribbloids.spookystuff.pipeline.SpookyTransformer
import com.tribbloids.spookystuff.sparkbinding.PageRowRDD
import com.tribbloids.spookystuff.{SpookyContext, dsl}


class GoogleImageTransformer(
                              override val uid: String =
                              classOf[GoogleImageTransformer].getCanonicalName + "_" + UUID.randomUUID().toString
                              ) extends SpookyTransformer {

  import dsl._
  import org.apache.spark.ml.param._

  /**
   * Param for input column name.
   * @group param
   */
  final val InputCol: Param[String] = new Param[String](this, "inputCol", "input column name")
  final val ImageUrlsCol: Param[String] = new Param[String](this, "ImageUrlCol", "output ImageUrlCol column name")
  //TODO: add scrolling down

  setDefault(ImageUrlsCol -> null)

  override def transform(dataset: PageRowRDD): PageRowRDD = {

    dataset.fetch(
      Visit("http://images.google.com/")
        +> TextInput("input[name=\"q\"]",toSymbol(InputCol))
        +> Submit("input[name=\"btnG\"]")
    ).select(
      S"div#search img".srcs ~ toSymbol(ImageUrlsCol)
    )
  }

  override def test(spooky: SpookyContext): Unit = {

    import org.apache.spark.sql.functions._
    import spooky.sqlContext.implicits._

    val source = spooky.create(Seq("Giant Robot"))

    val transformer = new GoogleImageTransformer() //TODO: change to copy
      .setInputCol("_")
      .setImageUrlsCol("uris")

    val result = transformer.transform(source)
    val df = result.flatten('uris ~ 'uri).toDF(sort = true).persist()

    assert(df.columns.toSeq == Seq("_", "uris", "uri"))
    df.collect().foreach(println)
    df.select('uris).map{
      v =>
        val arr = v.get(0).asInstanceOf[Array[String]]
        assert(arr.length >= 10)
    }
    df.select('uri).map{
      v =>
        val str = v.get(0).asInstanceOf[String]

        assert(str != null)
        assert(str.length >= 0)
    }

    assert(df.count() >= 10)
  }
}