package org.tribbloid.spookystuff.integration.scientific

import org.tribbloid.spookystuff.entity.client._
import org.tribbloid.spookystuff.integration.TestCore

/**
* Created by peng on 06/07/14.
*/
object GoogleScholar extends TestCore {

  import spooky._

  override def doMain() = {

    (sc.parallelize(Seq("Large scale distributed deep networks"))
      +> Visit("http://scholar.google.com/")
      +> DelayFor("form[role=search]")
      +> TextInput("input[name=\"q\"]","#{_}")
      +> Submit("button#gs_hp_tsb")
      +> DelayFor("div[role=main]")
      !=!())
      .extract(
        "title" -> (_.text1("div.gs_r h3.gs_rt a")),
        "citation" -> (_.text1("div.gs_r div.gs_ri div.gs_fl a:contains(Cited)"))
      )
      .visitJoin("div.gs_r div.gs_ri div.gs_fl a:contains(Cited)")(limit = 1)
      .paginate("div#gs_n td[align=left] a")()
      .sliceJoin("div.gs_r")()
      .extract(
        "citation_title" -> (_.text1("h3.gs_rt a")),
        "citation_abstract" -> (_.text1("div.gs_rs"))
      )
      .wgetJoin("div.gs_md_wp a")()
      .saveContent(select = _("citation_title"))
      .asSchemaRDD()
  }
}
