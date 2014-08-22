package org.tribbloid.spookystuff.example.largescale

import org.tribbloid.spookystuff.SpookyContext._
import org.tribbloid.spookystuff.entity._
import org.tribbloid.spookystuff.example.SparkSubmittable

/**
 * Created by peng on 06/07/14.
 */
object GoogleScholar extends SparkSubmittable {

  override def doMain() {

    (sc.parallelize(Seq("Large scale distributed deep networks")) +>
      Visit("http://scholar.google.com/") +>
      DelayFor("form[role=search]",50) +>
      TextInput("input[name=\"q\"]","#{_}") +>
      Submit("button#gs_hp_tsb") +>
      DelayFor("div[role=main]",50) !==).selectInto(
        "title" -> (_.text1("div.gs_r h3.gs_rt a")),
        "citation" -> (_.text1("div.gs_r div.gs_ri div.gs_fl a:contains(Cited)"))
      ).leftJoin(
        "div.gs_r div.gs_ri div.gs_fl a:contains(Cited)",1
      ).insertPagination(
        "div#gs_n td[align=left] a"
      ).leftJoinBySlice("div.gs_r").selectInto(
        "citation_title" -> (_.text1("h3.gs_rt a")),
        "citation_abstract" -> (_.text1("div.gs_rs"))
      ).wgetLeftJoin(
        "div.gs_md_wp a"
      ).saveAs(
        fileName = "#{citation_title}",
        dir = "file:///home/peng/scholar/"
      ).map(
        page => (
          page.context.get("_"),
          page.context.get("title"),
          page.context.get("citation"),
          page.context.get("citation_title"),
          page.context.get("citation_abstract")
          ).productIterator.toList.mkString("\t")
      ).saveAsTextFile("file:///home/peng/scholar/result")
  }
}
