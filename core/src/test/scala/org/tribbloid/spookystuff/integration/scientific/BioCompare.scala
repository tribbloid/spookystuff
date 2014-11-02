package org.tribbloid.spookystuff.integration.scientific

import org.apache.spark.sql.SchemaRDD
import org.tribbloid.spookystuff.entity.client._
import org.tribbloid.spookystuff.integration.TestCore

/**
 * Created by peng on 11/1/14.
 */
object BioCompare extends TestCore {
  
  import spooky._

  override def doMain(): SchemaRDD = {

    val ranges = (sc.parallelize(Seq(
      "http://www.biocompare.com/1997-BrowseCategory/browse/gb1/9776/num",
      "http://www.biocompare.com/1997-BrowseCategory/browse/gb1/9776/A",
      "http://www.biocompare.com/1997-BrowseCategory/browse/gb1/9776/B",
      "http://www.biocompare.com/1997-BrowseCategory/browse/gb1/9776/C",
      "http://www.biocompare.com/1997-BrowseCategory/browse/gb1/9776/D",
      "http://www.biocompare.com/1997-BrowseCategory/browse/gb1/9776/E",
      "http://www.biocompare.com/1997-BrowseCategory/browse/gb1/9776/F",
      "http://www.biocompare.com/1997-BrowseCategory/browse/gb1/9776/G",
      "http://www.biocompare.com/1997-BrowseCategory/browse/gb1/9776/H",
      "http://www.biocompare.com/1997-BrowseCategory/browse/gb1/9776/I",
      "http://www.biocompare.com/1997-BrowseCategory/browse/gb1/9776/J",
      "http://www.biocompare.com/1997-BrowseCategory/browse/gb1/9776/K",
      "http://www.biocompare.com/1997-BrowseCategory/browse/gb1/9776/L",
      "http://www.biocompare.com/1997-BrowseCategory/browse/gb1/9776/M",
      "http://www.biocompare.com/1997-BrowseCategory/browse/gb1/9776/N",
      "http://www.biocompare.com/1997-BrowseCategory/browse/gb1/9776/O",
      "http://www.biocompare.com/1997-BrowseCategory/browse/gb1/9776/P",
      "http://www.biocompare.com/1997-BrowseCategory/browse/gb1/9776/Q",
      "http://www.biocompare.com/1997-BrowseCategory/browse/gb1/9776/R",
      "http://www.biocompare.com/1997-BrowseCategory/browse/gb1/9776/S",
      "http://www.biocompare.com/1997-BrowseCategory/browse/gb1/9776/T",
      "http://www.biocompare.com/1997-BrowseCategory/browse/gb1/9776/U",
      "http://www.biocompare.com/1997-BrowseCategory/browse/gb1/9776/V",
      "http://www.biocompare.com/1997-BrowseCategory/browse/gb1/9776/W",
      "http://www.biocompare.com/1997-BrowseCategory/browse/gb1/9776/X",
      "http://www.biocompare.com/1997-BrowseCategory/browse/gb1/9776/Y",
      "http://www.biocompare.com/1997-BrowseCategory/browse/gb1/9776/Z"
    ),27)
      +> Visit("#{_}")
      !=!())
      .wgetJoin("div.guidedBrowseCurrentOptionsSegments a")(indexKey = "range_index")
      .extract(
        "range" -> (_.text1("h1"))
      ).persist()

    print(ranges.count())

    val categories = ranges
      .sliceJoin("div.guidedBrowseResults > ul > li a")(indexKey = "category_index")
      .extract(
        "category" -> (_.text1("*"))
      ).persist()

    print(categories.count())

    categories.asSchemaRDD()

    //      .visitJoin("div.guidedBrowseResults > ul > li a")()
    //      .visitJoin("div.vendorRow a.viewAll")()
    //      .paginate("ul.pages > li.next > a", wget = false)()
    //      .sliceJoin("tr.productRow")()
    //      .extract(
    //        "Product name" -> (_.text1("h5")),
    //        "Applications" -> (_.text1("td:nth-of-type(2)")),
    //        "Reactivity" -> (_.text1("td:nth-of-type(3)")),
    //        "Conjugate/Tag/Label" -> (_.text1("td:nth-of-type(4)")),
    //        "Quantity" -> (_.text1("td:nth-of-type(5)"))
    //      )
    //      .asSchemaRDD()
  }
}
