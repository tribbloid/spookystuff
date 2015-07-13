package org.tribbloid.spookystuff.example.image

import org.tribbloid.spookystuff.SpookyContext
import org.tribbloid.spookystuff.actions._
import org.tribbloid.spookystuff.dsl._
import org.tribbloid.spookystuff.example.QueryCore
import org.tribbloid.spookystuff.expressions.Expression
import org.tribbloid.spookystuff.pages.{Elements, Unstructured}

import scala.collection.SeqView

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