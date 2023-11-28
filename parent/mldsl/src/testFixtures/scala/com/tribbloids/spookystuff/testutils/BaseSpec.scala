package com.tribbloids.spookystuff.testutils

import org.json4s.jackson.JsonMethods
import org.json4s.{DefaultFormats, Extraction, JValue}

object BaseSpec {

  // from org.apache.spark.JsonTestUtils
  def assertValidDataInJson(validateJson: JValue, expectedJson: JValue): Unit = {

    import org.json4s._

    val Diff(c, a, d) = validateJson.diff(expectedJson)
    val validatePretty = JsonMethods.pretty(validateJson)
    val expectedPretty = JsonMethods.pretty(expectedJson)
    val errorMessage = s"Expected:\n$expectedPretty\nFound:\n$validatePretty"
    assert(c == JNothing, s"\n$errorMessage\nChanged:\n${JsonMethods.pretty(c)}")
    assert(a == JNothing, s"\n$errorMessage\nAdded:\n${JsonMethods.pretty(a)}")
    assert(d == JNothing, s"\n$errorMessage\nDeleted:\n${JsonMethods.pretty(d)}")
  }
}

trait BaseSpec extends ai.acyclic.prover.commons.testlib.BaseSpec {

  import BaseSpec._

  @transient implicit class _JsonStrView(str: String) {

    def jsonShouldBe(gd: String): Unit = {
      val selfJ = JsonMethods.parse(str)
      val gdJ = JsonMethods.parse(gd)

      assertValidDataInJson(selfJ, gdJ)
    }
  }

  @transient implicit class _MapView(map: Map[String, Any]) {

    def shouldBe(gd: Map[String, Any]): Unit = {
      val selfJ = Extraction.decompose(map)(DefaultFormats)
      val gdJ = Extraction.decompose(gd)(DefaultFormats)

      // TODO: can we use tree visualization capabilities of prover-commons?
      assertValidDataInJson(selfJ, gdJ)
    }
  }

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
