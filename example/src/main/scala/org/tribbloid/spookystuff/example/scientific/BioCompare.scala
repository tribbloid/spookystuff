package org.tribbloid.spookystuff.example.scientific

import org.apache.spark.sql.SchemaRDD
import org.tribbloid.spookystuff.actions._
import org.tribbloid.spookystuff.expressions._
import org.tribbloid.spookystuff.factory.driver.TorProxyFactory
import org.tribbloid.spookystuff.example.TestCore

/**
 * Created by peng on 11/1/14.
 */
object BioCompare extends TestCore {

  spooky.proxy = TorProxyFactory
  import spooky._
  import sql._

  override def doMain(): SchemaRDD = {

    val initials = Seq(
      //      "num",
      "A"
      //      "B",
      //      "C",
      //      "D",
      //      "E",
      //      "F",
      //      "G",
      //      "H",
      //      "I",
      //      "J",
      //      "K",
      //      "L",
      //      "M",
      //      "N",
      //      "O",
      //      "P",
      //      "Q",
      //      "R",
      //      "S",
      //      "T",
      //      "U",
      //      "V",
      //      "W",
      //      "X",
      //      "Y",
      //      "Z"
    )

    val ranges = sc.parallelize(initials)
      .fetch(
        Wget("http://www.biocompare.com/1997-BrowseCategory/browse/gb1/9776/#{_}")
      )
      .wgetJoin('* href "div.guidedBrowseCurrentOptionsSegments a", indexKey = 'range_index)()
      .extract(
        "range" -> (_.text1("h1"))
      ).persist()

      println(ranges.count())

    val categories = ranges
      .sliceJoin("div.guidedBrowseResults > ul > li a")(indexKey = 'category_index)
      .extract(
        "category" -> (_.text1("*")),
        "first_page_url" -> (_.href1("*"))
      ).persist()

    println(categories.count())

    val firstPages = categories
    .fetch(
        Wget("#{first_page_url}?vcmpv=true"),
        numPartitions = initials.length*2500
      )
      .extract(
        "category_header" -> (_.text1("h1"))
      )

    val allPages  = firstPages
      .paginate("ul.pages > li.next > a")(indexKey = 'page)
      .extract(
        "url" -> (_.resolvedUrl)
      )
      .persist()

    println(allPages.count())

    val sliced = allPages
      .sliceJoin("tr.productRow")(indexKey = 'row)
      .persist()

    println(sliced.count())

    val data = sliced
      .extract(
        "Product name" -> (_.text1("h5")),
        "Applications" -> (_.text1("td:nth-of-type(2)")),
        "Reactivity" -> (_.text1("td:nth-of-type(3)")),
        "Conjugate/Tag/Label" -> (_.text1("td:nth-of-type(4)")),
        "Quantity" -> (_.text1("td:nth-of-type(5)"))
      )
      .asSchemaRDD()

    data.orderBy('range_index.asc, 'category_index.asc, 'page.asc, 'row.asc)
  }
}