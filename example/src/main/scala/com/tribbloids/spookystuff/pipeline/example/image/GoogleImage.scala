package com.tribbloids.spookystuff.pipeline.example.image

import com.tribbloids.spookystuff.SpookyContext
import com.tribbloids.spookystuff.dsl._
import com.tribbloids.spookystuff.example.QueryCore
import com.tribbloids.spookystuff.pipeline.google.ImageSearchTransformer

/**
 * Created by peng on 10/06/14.
 */
object GoogleImage extends QueryCore {

  override def doMain(spooky: SpookyContext) = {
    import spooky.dsl._

    val input = sc.parallelize("Yale University,Havard University".split(",").map(v =>"logo "+v.trim))
    new ImageSearchTransformer().setInputCol("_").setImageUrisCol("uri")
      .transform(input)
      .select(x"%html ${
        'uri.slice(0,5).mkString("<table><tr><td>","</td><td>","</td></tr></table>")
      }" ~ 'logo)
      .remove('uri).toDF()
  }
}