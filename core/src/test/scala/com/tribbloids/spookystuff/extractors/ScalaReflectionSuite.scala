package com.tribbloids.spookystuff.extractors

import com.tribbloids.spookystuff.tests.TestMixin
import com.tribbloids.spookystuff.utils.{ScalaUDT, TypeUtils, UnreifiedScalaType}
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

class ExampleUDT extends ScalaUDT[Example]
@SQLUserDefinedType(udt = classOf[ExampleUDT])
class Example(
               override val a: String = "dummy",
               override val b: Int = 1
             ) extends GenericExample[Int](a, b)

//class Example2(
//                override val a: String = "dummy",
//                override val b: Option[Int] = Some(1)
//              ) extends GenericExample[Option[Int]](a, b)
//}

class ScalaReflectionSuite extends FunSuite with TestMixin {

  import com.tribbloids.spookystuff.utils.ImplicitUtils.DataTypeView

  lazy val exLit: Literal[_] = Literal(new Example())
  lazy val exType: DataType = UnreifiedScalaType.apply[Example]

  test("getMethodsByName should work on overloaded function") {

    val dynamic = ScalaDynamicExtractor(
      exLit,
      "fn",
      None
    )

    val paramss = dynamic.getMethodsByName(exType).map(_.paramss)

    paramss.mkString("\n").shouldBe (
      """
        |List(List(value x), List(value y, value z))
        |List(List(value i))
        |List()
      """.stripMargin
    )

    val returnTypes = dynamic.getMethodsByName(exType).map{
      TypeUtils.getParameter_ReturnTypes(_, exType.scalaType.tpe)
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
      exLit,
      "a",
      None
    )

    val paramss = dynamic.getMethodsByName(exType).map(_.paramss)

    paramss.mkString("\n").shouldBe(
      "List()"
    )

    val returnTypes = dynamic.getMethodsByName(exType).map{
      TypeUtils.getParameter_ReturnTypes(_, exType.scalaType.tpe)
    }
    returnTypes.mkString("\n").shouldBe (
      """
        |(List(),String)
      """.stripMargin
    )
  }

  test("getMethodsByName should work on lazy val property") {

    val dynamic = ScalaDynamicExtractor(
      exLit,
      "c",
      None
    )

    val paramss = dynamic.getMethodsByName(exType).map(_.paramss)

    paramss.mkString("\n").shouldBe(
      "List()"
    )

    val returnTypes = dynamic.getMethodsByName(exType).map{
      TypeUtils.getParameter_ReturnTypes(_, exType.scalaType.tpe)
    }
    returnTypes.mkString("\n").shouldBe (
      """
        |(List(),String)
      """.stripMargin
    )
  }

  test("getMethodsByName should work on function with default parameters") {

    val dynamic = ScalaDynamicExtractor(
      exLit,
      "fnDefault",
      None
    )

    val paramss = dynamic.getMethodsByName(exType).map(_.paramss)

    paramss.mkString("\n").shouldBe(
      "List(List(value a, value b))"
    )

    val returnTypes = dynamic.getMethodsByName(exType).map{
      TypeUtils.getParameter_ReturnTypes(_, exType.scalaType.tpe)
    }
    returnTypes.mkString("\n").shouldBe (
      """
        |(List(List(Int, String)),String)
      """.stripMargin
    )
  }

  test("getMethodsByName should work on operator") {

    val dynamic = ScalaDynamicExtractor(
      exLit,
      "*=>",
      None
    )

    val paramss = dynamic.getMethodsByName(exType).map(_.paramss)

    paramss.mkString("\n").shouldBe(
      "List(List(value k))"
    )

    val returnTypes = dynamic.getMethodsByName(exType).map{
      TypeUtils.getParameter_ReturnTypes(_, exType.scalaType.tpe)
    }
    returnTypes.mkString("\n").shouldBe (
      """
        |(List(List(Int)),String)
      """.stripMargin
    )
  }

  test("getMethodByScala should work on overloaded function") {
    val dynamic = ScalaDynamicExtractor(
      exLit,
      "fn",
      None
    )

    val paramss = dynamic.getMethodByScala(exType, Some(List(IntegerType))).paramss

    paramss.toString.shouldBe(
      "List(List(value i))"
    )
  }

  test("getMethodByJava should work on overloaded function") {
    val dynamic = ScalaDynamicExtractor(
      exLit,
      "fn",
      None
    )

    val paramss = dynamic.getMethodByJava(exType, Some(List(IntegerType)))
      .getParameterTypes
      .mkString("|")

    paramss.shouldBe(
      "class java.lang.Object"
    )
  }

  test("getMethodByScala should work on function with option output") {
    val dynamic = ScalaDynamicExtractor(
      exLit,
      "fnOpt",
      None
    )

    val paramss = dynamic.getMethodByScala(exType, Some(List(IntegerType))).paramss

    paramss.toString.shouldBe(
      "List(List(value x))"
    )
  }

  test("getMethodByJava should work on function with option output") {
    val dynamic = ScalaDynamicExtractor(
      exLit,
      "fnOpt",
      None
    )

    val paramss = dynamic.getMethodByJava(exType, Some(List(IntegerType)))
      .getParameterTypes.mkString("|")

    paramss.shouldBe(
      "class java.lang.Object"
    )
  }

  ignore("getMethodByScala should work on function with option parameter") {
    val dynamic = ScalaDynamicExtractor(
      exLit,
      "fnOptOpt",
      None
    )

    val paramss = dynamic.getMethodByScala(exType, Some(List(IntegerType))).paramss

    paramss.toString.shouldBe(
      "List(List(value x))"
    )
  }

  ignore("getMethodByJava should work on function with option parameter") {
    val dynamic = ScalaDynamicExtractor(
      exLit,
      "fnOptOpt",
      None
    )

    val paramss = dynamic.getMethodByJava(exType, Some(List(IntegerType)))
      .getParameterTypes.mkString("|")

    paramss.shouldBe(
      "class scala.Option"
    )
  }

  test("getMethodByScala should throw error if parameter Type is incorrect") {
    val dynamic = ScalaDynamicExtractor(
      exLit,
      "fn",
      None
    )

    intercept[UnsupportedOperationException] {
      val paramss = dynamic.getMethodByScala(exType, Some(List(StringType)))
    }
  }

  //TODO: this doesn't matter as its only used after scala reflection-based method, but should be fixed in the future
  ignore("getMethodByJava should return None if parameter Type is incorrect") {
    val dynamic = ScalaDynamicExtractor(
      exLit,
      "fn",
      None
    )

    intercept[UnsupportedOperationException] {
      val paramss = dynamic.getMethodByJava(exType, Some(List(StringType)))
    }
  }

  test("getMethodByScala should work on operator") {

    val dynamic = ScalaDynamicExtractor(
      exLit,
      "*=>",
      None
    )

    val method = dynamic.getMethodByScala(exType, Some(List(IntegerType)))
    val paramss = method.paramss

    paramss.toString.shouldBe(
      "List(List(value k))"
    )

    val returnType = TypeUtils.getParameter_ReturnTypes(method, exType.scalaType.tpe)
    returnType.toString.shouldBe (
      """
        |(List(List(Int)),String)
      """.stripMargin
    )
  }

  test("getMethodByJava should work on operator") {

    val dynamic = ScalaDynamicExtractor(
      exLit,
      "*=>",
      None
    )

    val method = dynamic.getMethodByJava(exType, Some(List(IntegerType)))
    val types = method.getParameterTypes.mkString("|")

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

  override lazy val exLit = Literal(new GenericExample[Int]("dummy", 1))
  //  val evi = (ex.dataType)
  override lazy val exType = UnreifiedScalaType.apply[GenericExample[Int]]
}