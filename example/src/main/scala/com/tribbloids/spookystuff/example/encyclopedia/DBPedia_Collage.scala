package com.tribbloids.spookystuff.example.encyclopedia

import com.tribbloids.spookystuff.SpookyContext
import com.tribbloids.spookystuff.actions._
import com.tribbloids.spookystuff.dsl._
import com.tribbloids.spookystuff.example.QueryCore

/**
 * Created by peng on 14/06/15.
 */
object DBPedia_Collage extends QueryCore {

  override def doMain(spooky: SpookyContext) = {

    import sql.implicits._

    val str = "Barack Obama"
    val cls = "person"

    DBPedia.imgPages(spooky, cls, str)
      .select(S"div#search img".code.replaceAll("<img", """<img style="display:inline"""") ~ 'image
      ).toDF(sort=true).select('image).map(_.getString(0)).collect().mkString("")
  }
}