package com.tribbloids.spookystuff.relay.xml

import com.tribbloids.spookystuff.relay.json.BaseFormats
import com.tribbloids.spookystuff.testutils.BaseSpec
import org.json4s.{DefaultFormats, Formats, JObject}

object XMLCompatSpec {
  case class StrStr(
      a: String,
      b: String
  )

  case class StrInt(
      a: String = "A",
      b: Int = 2
  )

  case class StrDbl(
      a: String,
      b: Double
  )

  case class StrIntArray(
      a: String,
      b: Array[Int]
  )

  case class StrIntSeq(
      a: String,
      b: Seq[Int]
  )

  case class StrIntSet(
      a: String,
      b: Set[Int]
  )

  case class OptInt(
      a: Option[Int]
  )
}

class XMLCompatSpec extends BaseSpec {

  implicit val formats: Formats = BaseFormats ++ XMLCompat.serializers

  import XMLCompatSpec.*
  import org.json4s.Extraction.*

  it("sanity test") {

    implicit val formats: Formats = DefaultFormats

    val d1 = StrInt("a", 12)
    val json = decompose(d1)

    assert(json != JObject(Nil))
  }

  it("int to String") {

    val d1 = StrInt("a", 12)
    val json = decompose(d1)

    val d2 = extract[StrStr](json)
    d2.toString.shouldBe("StrStr(a,12)")
  }

  it("string to int") {
    val d1 = StrStr("a", "12")
    val json = decompose(d1)

    val d2 = extract[StrInt](json)
    d2.toString.shouldBe("StrInt(a,12)")
  }

  it("double to int") {
    val d1 = StrDbl("a", 12.51)
    val json = decompose(d1)

    val d2 = extract[StrInt](json)
    d2.toString.shouldBe("StrInt(a,12)")
  }

  it("int to int array") {
    val d1 = StrInt("a", 12)
    val json = decompose(d1)

    val d2 = extract[StrIntArray](json)
    d2.copy(b = null).toString.shouldBe("StrIntArray(a,null)")
  }

  it("int array to int array") {
    val d1 = StrIntArray("a", Array(12))
    val json = decompose(d1)

    val d2 = extract[StrIntArray](json)
    d2.copy(b = null).toString.shouldBe("StrIntArray(a,null)")
  }

  it("int to int seq") {
    val d1 = StrInt("a", 12)
    val json = decompose(d1)

    val d2 = extract[StrIntSeq](json)
    d2.toString.shouldBe("StrIntSeq(a,List(12))")
  }

  it("int to int set") {
    val d1 = StrInt("a", 12)
    val json = decompose(d1)

    val d2 = extract[StrIntSet](json)
    d2.toString.shouldBe("StrIntSet(a,Set(12))")
  }

  it("string to int array") {
    val d1 = StrStr("a", "12")
    val json = decompose(d1)

    val d2 = extract[StrIntArray](json)
    d2.copy(b = null).toString.shouldBe("StrIntArray(a,null)")
  }

  it("string to int seq") {
    val d1 = StrStr("a", "12")
    val json = decompose(d1)

    val d2 = extract[StrIntSeq](json)
    d2.toString.shouldBe("StrIntSeq(a,List(12))")
  }

  it("string to int set") {
    val d1 = StrStr("a", "12")
    val json = decompose(d1)

    val d2 = extract[StrIntSet](json)
    d2.toString.shouldBe("StrIntSet(a,Set(12))")
  }

  it("empty string to Object") {
    val d1 = ""
    val json = decompose(d1)

    val d2 = extract[OptInt](json)
    d2.toString.shouldBe("OptInt(None)")
  }

  it("empty string to Option[Map]") {
    val d1 = ""
    val json = decompose(d1)

    val d2 = extract[Option[Map[String, String]]](json)
    d2.toString.shouldBe("Some(Map())")
  }

  // this is no longer consistent with the latest behaviour of json4s
  ignore("missing member to default constructor value") {
    val d1 = StrIntSeq("a", Nil)
    val json = decompose(d1)

    val d2 = extract[StrInt](json)
    d2.toString.shouldBe("StrInt(a,2)")
  }

  it("empty string to default constructor value") {
    val d1 = ""
    val json = decompose(d1)

    val d2 = extract[StrInt](json)
    d2.toString.shouldBe("StrInt(A,2)")
  }
}
