package com.tribbloids.spookystuff.testutils

import org.json4s.JValue
import org.json4s.jackson.JsonMethods
import org.scalatest.Suite

trait Suitex {
  self: Suite =>

  final val ACTUAL =
    "[ACTUAL   /  LEFT]"
  final val EXPECTED =
    "[EXPECTED / RIGHT]"

//  CommonUtils.debugCPResource()

  @transient implicit class TestStringView(str: String) {

    // TODO: use reflection to figure out test name and annotate
    def shouldBe(
        gd: String = null,
        sort: Boolean = false,
        ignoreCase: Boolean = false,
        superSet: Boolean = false
    ): Unit = {

      val rows = str
        .split("\n")
        .toList

      var a: List[String] = rows
        .filterNot(_.replaceAllLiterally(" ", "").isEmpty)
        .map(v => ("|" + v).trim.stripPrefix("|"))
      if (sort) a = a.sorted
      if (ignoreCase) a = a.map(_.toLowerCase)

      Option(gd) match {
        case None =>
          println(AssertionErrorObject(rows, null).actualInfo)
        case Some(_gd) =>
          var b = _gd
            .split("\n")
            .toList
            .filterNot(_.replaceAllLiterally(" ", "").isEmpty)
            .map(v => ("|" + v).trim.stripPrefix("|"))
          if (sort) b = b.sorted
          if (ignoreCase) b = b.map(_.toLowerCase)
          if (superSet) {
            Predef.assert(
              a.intersect(b).nonEmpty,
              AssertionErrorObject(a, b)
            )
          } else {
            Predef.assert(
              a == b,
              AssertionErrorObject(a, b)
            )
          }
      }
    }

    // ignore sequence
//    def jsonShouldBe(
//        gd: String = null,
//        mode: JSONCompareMode = JSONCompareMode.LENIENT
//    ): Unit = {
//
//      JSONAssert.assertEquals(str, gd, mode)
//    }

    def jsonShouldBe(gd: String): Unit = {
      val selfJ = JsonMethods.parse(str)
      val gdJ = JsonMethods.parse(gd)

      assertValidDataInJson(selfJ, gdJ)
    }

    def rowsShouldBe(
        gd: String = null
    ): Unit = shouldBe(gd, sort = true)

    def shouldBeLike(
        gd: String = null,
        sort: Boolean = false,
        ignoreCase: Boolean = false
    ): Unit = {
      val aRaw: List[String] = str
        .split("\n")
        .toList
        .filterNot(_.replaceAllLiterally(" ", "").isEmpty)
        .map(v => ("|" + v).trim.stripPrefix("|"))
      val a =
        if (sort) aRaw.sorted
        else aRaw

      Option(gd) match {
        case None =>
          println(AssertionErrorObject(a, null).actualInfo)
        case Some(_gd) =>
          var b = _gd
            .split("\n")
            .toList
            .filterNot(_.replaceAllLiterally(" ", "").isEmpty)
            .map(v => ("|" + v).trim.stripPrefix("|"))
          if (sort) b = b.sorted
          if (ignoreCase) b = b.map(_.toLowerCase)
          try {
            a.zipAll(b, null, null).foreach { tuple =>
              val fixes = tuple._2.split("[.]{6,}", 2)
              lazy val errorMsg = s"${tuple._1} doesn't look like ${tuple._2}"
              Predef.assert(
                tuple._1.startsWith(fixes.head),
                errorMsg
              )
              Predef.assert(
                tuple._1.endsWith(fixes.last),
                errorMsg
              )
            }
          } catch {
            case e: Exception =>
              throw new AssertionError("" + AssertionErrorObject(a, b), e)
          }
      }
    }

    def rowsShouldBeLike(gd: String = null): Unit = shouldBeLike(gd, sort = true)

    //    def uriContains(contains: String): Boolean = {
    //      str.contains(contains) &&
    //        str.contains(URLEncoder.encode(contains,"UTF-8"))
    //    }
    //
    //    def assertUriContains(contains: String): Unit = {
    //      assert(
    //        str.contains(contains) &&
    //        str.contains(URLEncoder.encode(contains,"UTF-8")),
    //        s"$str doesn't contain either:\n" +
    //          s"$contains OR\n" +
    //          s"${URLEncoder.encode(contains,"UTF-8")}"
    //      )
    //    }

    // from org.apache.spark.JsonTestUtils
    def assertValidDataInJson(validateJson: JValue, expectedJson: JValue): Unit = {

      import org.json4s._

      val Diff(c, a, d) = validateJson.diff(expectedJson)
      val validatePretty = JsonMethods.pretty(validateJson)
      val expectedPretty = JsonMethods.pretty(expectedJson)
      val errorMessage = s"Expected:\n$expectedPretty\nFound:\n$validatePretty"
//      import org.scalactic.TripleEquals._
      assert(c == JNothing, s"\n$errorMessage\nChanged:\n${JsonMethods.pretty(c)}")
      assert(a == JNothing, s"\n$errorMessage\nAdded:\n${JsonMethods.pretty(a)}")
      assert(d == JNothing, s"\n$errorMessage\nDeleted:\n${JsonMethods.pretty(d)}")
    }
  }

  // TODO: update to be on par with scalatest supported by IDE
  case class AssertionErrorObject(actual: List[String], expected: List[String]) {

    lazy val actualStr: String = actual.mkString("\n")
    lazy val actualInfo: String = s"\n=============================== $ACTUAL ================================\n\n" +
      actualStr

    lazy val expectedStr: String = expected.mkString("\n")
    lazy val expectedInfo: String =
      s"\n=============================== $EXPECTED ================================\n\n" +
        expectedStr

    override lazy val toString: String = {

      val result = s"""
         |"
         |$actualInfo
         |" did not equal "
         |$expectedInfo
         |"
      """.stripMargin.split('\n').filter(_.nonEmpty).mkString("\n")

      result
    }
  }

  @transient implicit class TestMapView[K, V](map: scala.collection.Map[K, V]) {

    assert(map != null)

    def shouldBe(expected: scala.collection.Map[K, V]): Unit = {

      val messages = expected.toSeq.flatMap { tuple =>
        val messageOpt = map.get(tuple._1) match {
          case None =>
            Some(s"${tuple._1} doesn't exist in map")
          case Some(v) =>
            if (v == tuple._2) None
            else Some(s"${tuple._1} mismatch: expected ${tuple._2} =/= actual $v")
        }
        messageOpt
      }

      if (messages.nonEmpty)
        throw new AssertionError("Assertion failure: {\n" + messages.mkString("\n") + "\n}")
    }

    def shouldBe(expected: (K, V)*): Unit = {
      this.shouldBe(Map(expected: _*))
    }
  }

  def bypass(f: => Unit): Unit = {}

  //  override def intercept[T <: AnyRef](f: => Any)(implicit manifest: Manifest[T]): T = {
  //    super.intercept{
  //      try f
  //      catch {
  //        case e: Exception =>
  //          println("Attempt to intercept:")
  //          e.printStackTrace()
  //          throw e
  //      }
  //    }
  //  }
}
