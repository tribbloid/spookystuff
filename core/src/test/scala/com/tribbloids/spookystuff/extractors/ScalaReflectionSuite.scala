package com.tribbloids.spookystuff.extractors

import com.tribbloids.spookystuff.extractors.ScalaReflectionSuite.Example
import com.tribbloids.spookystuff.tests.TestMixin
import org.apache.spark.sql.TypeUtils
import org.apache.spark.sql.catalyst.ScalaReflection.universe._
import org.scalatest.FunSuite

object ScalaReflectionSuite {

  class GenericExample[T](
                           val a: String,
                           val b: T
                         ) {

    lazy val c = a + b

    def fn: T = b
    def fn(i: T) = "" + b + i

    def fn(x: T)(y: T, z: T) = "" + b + x + y + z

    def withDefaultParam(
                          a: T,
                          b: String = "b"
                        ) = "" + a + b

    def *=>(k: T): String = "" + k
  }

  case class Example(
                      override val a: String,
                      override val b: Int
                    ) extends GenericExample[Int](a, b)
}

class ScalaReflectionSuite extends FunSuite with TestMixin {

  lazy val ex: Literal[_] = Literal(Example("dummy", 1))
  //  val evi = TypeEvidence(ex.dataType)
  lazy val evi = TypeEvidence(typeTag[Example])

  test("getMethods should work on overloaded function") {

    val dynamic = ScalaDynamicExtractor(
      ex,
      "fn",
      Nil
    )

    val paramss = dynamic._getAllMethods(evi).map(_.paramss)

    paramss.mkString("\n").shouldBe (
      """
        |List(List(value x), List(value y, value z))
        |List(List(value i))
        |List()
      """.stripMargin
    )

    val returnTypes = dynamic._getAllMethods(evi).map{
      TypeUtils.methodSymbolToParameter_Returntypes(_, evi.scalaType.tpe)
    }
    returnTypes.mkString("\n").shouldBe (
      """
        |(List(List(Int), List(Int, Int)),String)
        |(List(List(Int)),String)
        |(List(),Int)
      """.stripMargin
    )
  }

  test("getMethods should work on case constructor parameter") {

    val dynamic = ScalaDynamicExtractor (
      ex,
      "a",
      Nil
    )

    val paramss = dynamic._getAllMethods(evi).map(_.paramss)

    paramss.mkString("\n").shouldBe(
      "List()"
    )

    val returnTypes = dynamic._getAllMethods(evi).map{
      TypeUtils.methodSymbolToParameter_Returntypes(_, evi.scalaType.tpe)
    }
    returnTypes.mkString("\n").shouldBe (
      """
        |(List(),String)
      """.stripMargin
    )
  }

  test("getMethods should work on lazy val property") {

    val dynamic = ScalaDynamicExtractor(
      ex,
      "c",
      Nil
    )

    val paramss = dynamic._getAllMethods(evi).map(_.paramss)

    paramss.mkString("\n").shouldBe(
      "List()"
    )

    val returnTypes = dynamic._getAllMethods(evi).map{
      TypeUtils.methodSymbolToParameter_Returntypes(_, evi.scalaType.tpe)
    }
    returnTypes.mkString("\n").shouldBe (
      """
        |(List(),String)
      """.stripMargin
    )
  }

  test("getMethods should work on function with default parameters") {

    val dynamic = ScalaDynamicExtractor(
      ex,
      "withDefaultParam",
      Nil
    )

    val paramss = dynamic._getAllMethods(evi).map(_.paramss)

    paramss.mkString("\n").shouldBe(
      "List(List(value a, value b))"
    )

    val returnTypes = dynamic._getAllMethods(evi).map{
      TypeUtils.methodSymbolToParameter_Returntypes(_, evi.scalaType.tpe)
    }
    returnTypes.mkString("\n").shouldBe (
      """
        |(List(List(Int, String)),String)
      """.stripMargin
    )
  }

  test("getMethods should work on operator") {

    val dynamic = ScalaDynamicExtractor(
      ex,
      "*=>",
      Nil
    )

    val paramss = dynamic._getAllMethods(evi).map(_.paramss)

    paramss.mkString("\n").shouldBe(
      "List(List(value k))"
    )

    val returnTypes = dynamic._getAllMethods(evi).map{
      TypeUtils.methodSymbolToParameter_Returntypes(_, evi.scalaType.tpe)
    }
    returnTypes.mkString("\n").shouldBe (
      """
        |(List(List(Int)),String)
      """.stripMargin
    )
  }

  test("getScalaReflectionMethod should work on overloaded function") {
    val dynamic = ScalaDynamicExtractor(
      ex,
      "fn",
      Nil
    )

    val paramss = dynamic.getScalaReflectionMethod(evi, List(List(TypeEvidence(typeTag[Int])))).map(_.paramss)

    paramss.toSeq.mkString("\n").shouldBe(
      "List(List(value i))"
    )
  }

  test("getScalaReflectionMethod should return None if parameter Type is incorrect") {
    val dynamic = ScalaDynamicExtractor(
      ex,
      "fn",
      Nil
    )

    val paramss = dynamic.getScalaReflectionMethod(evi, List(List(TypeEvidence(typeTag[String])))).map(_.paramss)

    assert(paramss.isEmpty)
  }

  //  test("resolveType should work on ")

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

class ScalaReflectionSuite_Generic extends ScalaReflectionSuite {

  import ScalaReflectionSuite._

  override lazy val ex = Literal(new GenericExample[Int]("dummy", 1))
  //  val evi = TypeEvidence(ex.dataType)
  override lazy val evi = TypeEvidence(typeTag[GenericExample[Int]])
}