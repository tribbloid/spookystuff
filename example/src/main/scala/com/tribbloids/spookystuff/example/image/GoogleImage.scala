package com.tribbloids.spookystuff.example.image

import com.tribbloids.spookystuff.SpookyContext
import com.tribbloids.spookystuff.actions._
import com.tribbloids.spookystuff.dsl._
import com.tribbloids.spookystuff.example.QueryCore

/**
 * Created by peng on 10/06/14.
 */
object GoogleImage extends QueryCore {

  override def doMain(spooky: SpookyContext) = {
    import spooky.dsl._

    sc.parallelize("Yale University,Havard University".split(",").map(_.trim)).fetch(
      Visit("http://images.google.com/")
        +> TextInput("input[name=\"q\"]","Logo '{_}")
        +> Submit("input[name=\"btnG\"]")
    ).select(x"%html ${
        S"div#search img".slice(0,5).codes.mkString("<table><tr><td>","</td><td>","</td></tr></table>")
    }" ~ 'logo).toDF()
  }
}