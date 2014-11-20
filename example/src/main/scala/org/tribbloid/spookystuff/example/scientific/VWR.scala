package org.tribbloid.spookystuff.example.scientific

import org.apache.spark.sql.SchemaRDD
import org.tribbloid.spookystuff.SpookyContext
import org.tribbloid.spookystuff.actions._
import org.tribbloid.spookystuff.expressions._
import org.tribbloid.spookystuff.example.ExampleCore

/**
 * Created by peng on 11/19/14.
 */
object VWR extends ExampleCore {

  override def doMain(spooky: SpookyContext): SchemaRDD = {

    import spooky._

    val urlEntry = "https://ca.vwr.com/store/content/externalContentPage.jsp?path=/en_CA/life_science_main.jsp"

    noInput
      .fetch(
        Visit(urlEntry)
      )
      .sliceJoin("div#leftnav > ul > li > ul > li")(indexKey = 'r)
      .visitJoin('*.href("a"))()//go catelog
      .sliceJoin("ul.niveau1 li")(indexKey = 'r)
      //     .visitJoin('*.href("a"))()//go catelog
      .join('*.href("a") as '~)(
        Visit("#{~}")
          //+>Try(Click("#GoTo64") :: Nil)
          +>Paginate("a[title=Next]"),
        pageIndexKey = 'page
      )
      .extract(
        "url" -> (_.url),
        "expired" ->(_.text1("div div h2"))
      )
      .sliceJoin("div.w99 > table:nth-child(5) > tbody > tr td" )()
      .extract(
        "name" -> (_.text1("div.desc"))
      )
      .asSchemaRDD()
  }
}