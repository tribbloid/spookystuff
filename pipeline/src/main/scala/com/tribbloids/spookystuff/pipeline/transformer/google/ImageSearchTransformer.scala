package com.tribbloids.spookystuff.pipeline.transformer.google

import java.util.UUID

import com.tribbloids.spookystuff.actions._
import com.tribbloids.spookystuff.pipeline.SpookyTransformer
import com.tribbloids.spookystuff.sparkbinding.PageRowRDD
import com.tribbloids.spookystuff.{SpookyContext, dsl}


class ImageSearchTransformer(
                              override val uid: String =
                              classOf[ImageSearchTransformer].getCanonicalName + "_" + UUID.randomUUID().toString
                              ) extends SpookyTransformer {

  import dsl._
  import org.apache.spark.ml.param._

  /**
   * Param for input column name.
   * @group param
   */
  final val InputCol: Param[String] = new Param[String](this, "inputCol", "input column name")
  final val ImageUrisCol: Param[String] = new Param[String](this, "ImageUrisCol", "output ImageUrisCol column name")
  //TODO: add scrolling down

  setDefault(ImageUrisCol -> null)

  override def transform(dataset: PageRowRDD): PageRowRDD = {

    dataset.fetch(
      Visit("http://images.google.com/")
        +> TextInput("input[name=\"q\"]",toSymbol(InputCol))
        +> Submit("input[name=\"btnG\"]")
    ).select(
      S"div#search img".srcs ~ toSymbol(ImageUrisCol)
    )
  }

  override def test(spooky: SpookyContext): Unit = {

    import spooky.sqlContext.implicits._

    val source = spooky.create(Seq("Giant Robot", "Small Robot"))

    val transformer = new ImageSearchTransformer() //TODO: change to copy
      .setInputCol("_")
      .setImageUrisCol("uris")

    val result = transformer.transform(source)
    val df = result.toDF(sort = true).persist()

    assert(df.columns.toSeq == Seq("_", "uris"))
    df.collect().foreach(println)
    assert(df.count() == 2)
    df.select('uris).map{
      v =>
        val arr = v.getAs[Array[String]](0)
        assert(arr.length >= 10)
    }
  }
}