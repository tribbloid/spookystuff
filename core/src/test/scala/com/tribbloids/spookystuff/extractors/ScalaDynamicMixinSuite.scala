package com.tribbloids.spookystuff.extractors

import com.tribbloids.spookystuff.SpookyEnvSuite
import org.apache.spark.sql.catalyst.ScalaReflection.universe._

/**
  * Created by peng on 15/06/16.
  */
class GenericExample[T](
                         a: String,
                         b: T
                       ) {

  lazy val c = a + b

  def fn: String = a
  def fn(i: T) = "" + b + i

  def fn(x: T)(y: T, z: T) = "" + b + x + y + z

  def withDefaultParam(
                        a: T,
                        b: String = "b"
                      ) = "" + a + b
}

case class Example(
                    a: String,
                    b: Int
                  ) extends GenericExample[Int](a, b)

class ScalaDynamicMixinSuite extends SpookyEnvSuite {

  val ex = Literal(10)

  test("getMethods should work on overloaded function") {

    val dynamic = ScalaDynamicExtractor(
      ex,
      "fn",
      Nil
    )

    val paramss = dynamic.getMethods(TypeEvidence(typeTag[Example])).map(_.paramss)

    paramss.mkString("\n").shouldBe(
      """
        |List(List(value x), List(value y, value z))
        |List(List(value i))
        |List()
      """.stripMargin
    )

//    val types = paramss.flatten.flatten.map(
//      _.typeSignature
//    )
  }

  test("getMethods should work on case constructor parameter") {

    val dynamic = ScalaDynamicExtractor(
      ex,
      "a",
      Nil
    )

    val paramss = dynamic.getMethods(TypeEvidence(typeTag[Example])).map(_.paramss)

    paramss.mkString("\n").shouldBe(
      "List()"
    )
  }

  test("getMethods should work on lazy val property") {

    val dynamic = ScalaDynamicExtractor(
      ex,
      "c",
      Nil
    )

    val paramss = dynamic.getMethods(TypeEvidence(typeTag[Example])).map(_.paramss)

    paramss.mkString("\n").shouldBe(
      "List()"
    )
  }

  test("getMethods should work on function with default parameters") {

    val dynamic = ScalaDynamicExtractor(
      ex,
      "withDefaultParam",
      Nil
    )

    val paramss = dynamic.getMethods(TypeEvidence(typeTag[Example])).map(_.paramss)

    paramss.mkString("\n").shouldBe(
      "List(List(value a, value b))"
    )
  }

  test("getValidMethod should work on overloaded function") {
    val dynamic = ScalaDynamicExtractor(
      ex,
      "fn",
      Nil
    )

    val paramss = dynamic.getValidMethod(TypeEvidence(typeTag[Example]), List(List(TypeEvidence(typeTag[Int])))).map(_.paramss)

    paramss.mkString("\n").shouldBe(
      "List(List(value i))"
    )
  }

  test("getValidMethod should return None if parameter Type is incorrect") {
    val dynamic = ScalaDynamicExtractor(
      ex,
      "fn",
      Nil
    )

    val paramss = dynamic.getValidMethod(TypeEvidence(typeTag[Example]), List(List(TypeEvidence(typeTag[String])))).map(_.paramss)

    assert(paramss.isEmpty)
  }

  //  test("Dynamic functions should be applicable on values") {
  //    val dataRow = DataRow(ListMap(Field("K1") -> "a,b,c", Field("K2") -> 2))
  //    val pageRow = FetchedRow(dataRow, Seq[Fetched]())
  //
  //    assert(dynamic('K1).isDefinedAt(pageRow))
  //    assert(dynamic('K1).apply(pageRow) == "a,b,c")
  //    val afterDynamic: Extractor[Any] = dynamic('K1).split(",")
  //    val afterDynamicValue = afterDynamic.apply(pageRow)
  //    assert(afterDynamicValue.asInstanceOf[Array[String]].toSeq == "a,b,c".split(",").toSeq)
  //  }
  //
  //  test("Dynamic functions should be applicable on expressions") {
  //    val dataRow = DataRow(ListMap(Field("K1") -> "a,b,c", Field("K2") -> ","))
  //    val pageRow = dataRow -> Seq[Fetched]()
  //    val afterDynamic: Extractor[Any] = dynamic('K1).split(dynamic('K2))
  //    val afterDynamicValue = afterDynamic.apply(pageRow)
  //    assert(afterDynamicValue.asInstanceOf[Array[String]].toSeq == "a,b,c".split(",").toSeq)
  //  }
}
