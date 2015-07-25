package org.tribbloid.spookystuff.example.encyclopedia

import org.tribbloid.spookystuff.SpookyContext
import org.tribbloid.spookystuff.actions._
import org.tribbloid.spookystuff.dsl._
import org.tribbloid.spookystuff.example.QueryCore

/**
 * Created by peng on 14/06/15.
 */
object DBPedia_Collage extends QueryCore {

  override def doMain(spooky: SpookyContext) = {

    import sql.implicits._

    val str = "Rob Ford"
    val cls = "person"

    DBPedia_Image.imgPages(spooky, cls, str)
      .select(S"div#search img".code.replaceAll("<img", """<img style="display:inline"""") ~ 'image
      ).toDF(sort=true).select('image).map(_.getString(0)).collect().mkString("")
  }
}