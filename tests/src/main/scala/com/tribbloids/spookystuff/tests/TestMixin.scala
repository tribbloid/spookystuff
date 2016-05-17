package com.tribbloids.spookystuff.tests

/**
  * Created by peng on 17/05/16.
  */
trait TestMixin {

  implicit class StringView(str: String) {

    //TODO: use reflection to figure out test name and annotate
    def shouldBe(gd: String = null): Unit = {
      val a = str.split("\n").toList.filterNot(_.replaceAllLiterally(" ","").isEmpty)
        .map(v => ("|" + v).trim.stripPrefix("|"))
      println("================================[ORIGINAL]=================================")
      println(a.mkString("\n"))

      Option(gd) match {
        case None =>

        case Some(_gd) =>
          val b = _gd.split("\n").toList.filterNot(_.replaceAllLiterally(" ","").isEmpty)
            .map(v => ("|" + v).trim.stripPrefix("|"))
          //          val patch = DiffUtils.diff(a, b)
          //          val unified = DiffUtils.generateUnifiedDiff("Output", "GroundTruth", a, patch, 1)
          //
          //          unified.asScala.foreach(println)
          assert(
            a == b,
            {
              "\n==============================[GROUND TRUTH]===============================\n" +
                b.mkString("\n") + "\n"
            }
          )
      }
    }
  }
}
