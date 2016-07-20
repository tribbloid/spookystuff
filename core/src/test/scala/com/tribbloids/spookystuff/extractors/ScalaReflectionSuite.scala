package com.tribbloids.spookystuff.extractors

import com.tribbloids.spookystuff.tests.TestMixin
import com.tribbloids.spookystuff.utils.TaggedUDT
import org.apache.spark.sql.TypeUtils
import org.apache.spark.sql.catalyst.ScalaReflection.universe._
import org.apache.spark.sql.types.{IntegerType, SQLUserDefinedType, StringType}
import org.scalatest.FunSuite

//object ScalaReflectionSuite {

class GenericExample[T](
                         val a: String,
                         val b: T
                       ) extends Serializable {

  lazy val c = a + b

  def fn: T = b
  def fn(i: T) = "" + b + i
  def fn(x: T)(y: T, z: T) = "" + b + x + y + z

  def fnOpt(x: T): Option[T] = {
    if (b == x) Some(x)
    else None
  }
  def fnOptOpt(x: Option[T]): Option[T] = {
    if (Option(b) == x) x
    else None
  }

  def fnDefault(
                 a: T,
                 b: String = "b"
               ) = "" + a + b

  def *=>(k: T): String = "" + k
}

class ExampleUDT extends TaggedUDT[Example]
@SQLUserDefinedType(udt = classOf[ExampleUDT])
class Example(
               override val a: String = "dummy",
               override val b: Int = 1
             ) extends GenericExample[Int](a, b)

class Example2(
                override val a: String = "dummy",
                override val b: Option[Int] = Some(1)
              ) extends GenericExample[Option[Int]](a, b)
//}

class ScalaReflectionSuite extends FunSuite with TestMixin {

  lazy val ex: Literal[_] = Literal(new Example())
  lazy val exampleEvi = TypeEvidence(typeTag[Example])

  test("getMethodsByName should work on overloaded function") {

    val dynamic = ScalaDynamicExtractor(
      ex,
      "fn",
      None
    )

    val paramss = dynamic.getMethodsByName(exampleEvi).map(_.paramss)

    paramss.mkString("\n").shouldBe (
      """
        |List(List(value x), List(value y, value z))
        |List(List(value i))
        |List()
      """.stripMargin
    )

    val returnTypes = dynamic.getMethodsByName(exampleEvi).map{
      TypeUtils.getParameter_ReturnTypes(_, exampleEvi.baseScalaType.tpe)
    }
    returnTypes.mkString("\n").shouldBe (
      """
        |(List(List(Int), List(Int, Int)),String)
        |(List(List(Int)),String)
        |(List(),Int)
      """.stripMargin
    )
  }

  test("getMethodsByName should work on case constructor parameter") {

    val dynamic = ScalaDynamicExtractor (
      ex,
      "a",
      None
    )

    val paramss = dynamic.getMethodsByName(exampleEvi).map(_.paramss)

    paramss.mkString("\n").shouldBe(
      "List()"
    )

    val returnTypes = dynamic.getMethodsByName(exampleEvi).map{
      TypeUtils.getParameter_ReturnTypes(_, exampleEvi.baseScalaType.tpe)
    }
    returnTypes.mkString("\n").shouldBe (
      """
        |(List(),String)
      """.stripMargin
    )
  }

  test("getMethodsByName should work on lazy val property") {

    val dynamic = ScalaDynamicExtractor(
      ex,
      "c",
      None
    )

    val paramss = dynamic.getMethodsByName(exampleEvi).map(_.paramss)

    paramss.mkString("\n").shouldBe(
      "List()"
    )

    val returnTypes = dynamic.getMethodsByName(exampleEvi).map{
      TypeUtils.getParameter_ReturnTypes(_, exampleEvi.baseScalaType.tpe)
    }
    returnTypes.mkString("\n").shouldBe (
      """
        |(List(),String)
      """.stripMargin
    )
  }

  test("getMethodsByName should work on function with default parameters") {

    val dynamic = ScalaDynamicExtractor(
      ex,
      "fnDefault",
      None
    )

    val paramss = dynamic.getMethodsByName(exampleEvi).map(_.paramss)

    paramss.mkString("\n").shouldBe(
      "List(List(value a, value b))"
    )

    val returnTypes = dynamic.getMethodsByName(exampleEvi).map{
      TypeUtils.getParameter_ReturnTypes(_, exampleEvi.baseScalaType.tpe)
    }
    returnTypes.mkString("\n").shouldBe (
      """
        |(List(List(Int, String)),String)
      """.stripMargin
    )
  }

  test("getMethodsByName should work on operator") {

    val dynamic = ScalaDynamicExtractor(
      ex,
      "*=>",
      None
    )

    val paramss = dynamic.getMethodsByName(exampleEvi).map(_.paramss)

    paramss.mkString("\n").shouldBe(
      "List(List(value k))"
    )

    val returnTypes = dynamic.getMethodsByName(exampleEvi).map{
      TypeUtils.getParameter_ReturnTypes(_, exampleEvi.baseScalaType.tpe)
    }
    returnTypes.mkString("\n").shouldBe (
      """
        |(List(List(Int)),String)
      """.stripMargin
    )
  }

  test("getMethodByScala should work on overloaded function") {
    val dynamic = ScalaDynamicExtractor(
      ex,
      "fn",
      None
    )

    val paramss = dynamic.getMethodByScala(exampleEvi, Some(List(TypeEvidence(IntegerType)))).paramss

    paramss.toString.shouldBe(
      "List(List(value i))"
    )
  }

  test("getMethodByJava should work on overloaded function") {
    val dynamic = ScalaDynamicExtractor(
      ex,
      "fn",
      None
    )

    val paramss = dynamic.getMethodByJava(exampleEvi, Some(List(TypeEvidence(IntegerType))))
      .getParameters.map(_.getType).mkString("|")

    paramss.shouldBe(
      "class java.lang.Object"
    )
  }

  test("getMethodByScala should work on function with option output") {
    val dynamic = ScalaDynamicExtractor(
      ex,
      "fnOpt",
      None
    )

    val paramss = dynamic.getMethodByScala(exampleEvi, Some(List(TypeEvidence(IntegerType)))).paramss

    paramss.toString.shouldBe(
      "List(List(value x))"
    )
  }

  test("getMethodByJava should work on function with option output") {
    val dynamic = ScalaDynamicExtractor(
      ex,
      "fnOpt",
      None
    )

    val paramss = dynamic.getMethodByJava(exampleEvi, Some(List(TypeEvidence(IntegerType))))
      .getParameters.map(_.getType).mkString("|")

    paramss.shouldBe(
      "class java.lang.Object"
    )
  }

  ignore("getMethodByScala should work on function with option parameter") {
    val dynamic = ScalaDynamicExtractor(
      ex,
      "fnOptOpt",
      None
    )

    val paramss = dynamic.getMethodByScala(exampleEvi, Some(List(TypeEvidence(IntegerType)))).paramss

    paramss.toString.shouldBe(
      "List(List(value x))"
    )
  }

  ignore("getMethodByJava should work on function with option parameter") {
    val dynamic = ScalaDynamicExtractor(
      ex,
      "fnOptOpt",
      None
    )

    val paramss = dynamic.getMethodByJava(exampleEvi, Some(List(TypeEvidence(IntegerType))))
      .getParameters.map(_.getType).mkString("|")

    paramss.shouldBe(
      "class scala.Option"
    )
  }

  test("getMethodByScala should throw error if parameter Type is incorrect") {
    val dynamic = ScalaDynamicExtractor(
      ex,
      "fn",
      None
    )

    intercept[UnsupportedOperationException] {
      val paramss = dynamic.getMethodByScala(exampleEvi, Some(List(TypeEvidence(StringType))))
    }
  }

  //TODO: this doesn't matter as its only used after scala reflection-based method, but should be fixed in the future
  ignore("getMethodByJava should return None if parameter Type is incorrect") {
    val dynamic = ScalaDynamicExtractor(
      ex,
      "fn",
      None
    )

    intercept[UnsupportedOperationException] {
      val paramss = dynamic.getMethodByJava(exampleEvi, Some(List(TypeEvidence(StringType))))
    }
  }

  test("getMethodByScala should work on operator") {

    val dynamic = ScalaDynamicExtractor(
      ex,
      "*=>",
      None
    )

    val method = dynamic.getMethodByScala(exampleEvi, Some(List(TypeEvidence(IntegerType))))
    val paramss = method.paramss

    paramss.toString.shouldBe(
      "List(List(value k))"
    )

    val returnType = TypeUtils.getParameter_ReturnTypes(method, exampleEvi.baseScalaType.tpe)
    returnType.toString.shouldBe (
      """
        |(List(List(Int)),String)
      """.stripMargin
    )
  }

  test("getMethodByJava should work on operator") {

    val dynamic = ScalaDynamicExtractor(
      ex,
      "*=>",
      None
    )

    val method = dynamic.getMethodByJava(exampleEvi, Some(List(TypeEvidence(IntegerType))))
    val types = method.getParameters.map(_.getType).mkString("|")

    types.toString.shouldBe(
      "class java.lang.Object"
    )
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

  override lazy val ex = Literal(new GenericExample[Int]("dummy", 1))
  //  val evi = TypeEvidence(ex.dataType)
  override lazy val exampleEvi = TypeEvidence(typeTag[GenericExample[Int]])
}