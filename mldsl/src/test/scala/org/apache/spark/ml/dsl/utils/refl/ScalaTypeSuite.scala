package org.apache.spark.ml.dsl.utils.refl

import com.tribbloids.spookystuff.testbeans.{Example, ExampleUDT}
import com.tribbloids.spookystuff.testutils.{FunSpecx, TestHelper}
import org.apache.spark.ml.dsl.utils.messaging.{Multipart, User}
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenContext
import org.apache.spark.sql.types._

/**
  * Created by peng on 08/06/16.
  */
class ScalaTypeSuite extends FunSpecx {

  import ScalaReflection.universe._
  import org.apache.spark.ml.dsl.utils.refl.ScalaType._

  it("UnreifiedScalaType.toString") {

    val tt = UnreifiedScalaType(typeTag[Int])
    tt.toString.shouldBe(
      "(unreified) TypeTag[Int]"
    )
  }

  /**
    * please keep this test to quickly identify any potential problems caused by changes in scala reflection API in the future
    */
  it("scala reflection can be used to get type of Array[String].headOption") {

    val arr: Seq[String] = Seq("abc", "def")
    val cls = arr.head.getClass
    val ttg: TypeTag[Seq[String]] = TypeUtils.getTypeTag(arr)
    val fns = ttg.tpe
      .members
    val fn = fns
      .filter(_.name.toString == "head")
      .head                           // Unsafely access it for now, use Option and map under normal conditions

    val fnTp: Type = fn.typeSignatureIn(ttg.tpe)

    val clsTp = fnTp.typeSymbol.asClass
    val fnRetTp = fnTp.asInstanceOf[ScalaReflection.universe.NullaryMethodType].resultType

    val fnCls = ttg.mirror.runtimeClass(fnRetTp)
    val fnCls2 = ttg.mirror.runtimeClass(clsTp)

    assert(cls == fnCls)
    assert(cls == fnCls2)
  }

  //  test("atomicTypePairs works as intended") {
  //    TypeUtils.atomicTypePairs.foreach(println)
  //  }

  val tupleSchema = StructType(Array(StructField("_1", IntegerType, nullable = false), StructField("_2", StringType, nullable = true)))

  describe("CatalystType <=> TypeTag") {
    val catalystType_scalaType: Seq[(DataType, TypeTag[_])] = Seq(
      StringType -> typeTag[String],
      IntegerType -> typeTag[Int],
      DoubleType -> typeTag[Double],
      ArrayType(DoubleType, containsNull = false) -> typeTag[Array[Double]],
      //    ArrayType(DoubleType, containsNull = true) -> typeTag[Array[Option[Double]]],
      ArrayType(StringType, containsNull = true) -> typeTag[Array[String]],
      //    ArrayType(StringType, containsNull = true) -> typeTag[Array[(String, Int)]],

      //    tupleSchema -> typeTag[(Int, String)], //TODO: urge spark team to fix the bug and re-enable it
      //    new ActionUDT -> typeTag[Action],
      new ExampleUDT -> typeTag[Example]
    )

    catalystType_scalaType.foreach {
      pair =>
        it(s"scalaType (${pair._2.tpe}) => catalystType (${pair._1})") {
          val converted = pair._2.tryReify.toOption
          println(converted)
          assert(converted == Some(pair._1))
        }

        it(s"catalystType (${pair._1}) => scalaType (${pair._2.tpe})") {
          val converted = pair._1.scalaTypeOpt
          println(converted)
          assert(converted.map(_.asClass) == Some(pair._2.asClass))
        }

        //TODO: this failed on CI for UDT with unknown reason, why?
        ignore(s"CodeGenerator.javaType(${pair._1})") {
          val genCtx = new CodegenContext
          pair._1 match {
            case v: UserDefinedType[_] =>
              println(s"UDT: ${pair._1.getClass.getCanonicalName}")
              assert(v.sqlType == BinaryType)
            case _ =>
          }
          val tt = genCtx.javaType(pair._1)
          assert(tt.toLowerCase() != "object")
        }

      //TODO: add 1 test to ensure that ScalaUDT can be used in DataFrame with codegen.
    }
  }

  describe("CatalystType => TypeTag") {
    val oneWayPairs: Seq[(TypeTag[_], DataType)] = Seq(
      typeTag[Array[(String, Int)]] -> ArrayType(StructType(Seq(StructField("_1", StringType), StructField("_2", IntegerType, nullable = false))), containsNull = true),
      typeTag[Seq[(String, Int)]] -> ArrayType(StructType(Seq(StructField("_1", StringType), StructField("_2", IntegerType, nullable = false))), containsNull = true)
      //    typeTag[Set[(String, Int)]] -> ArrayType(StructType(Seq(StructField("_1", StringType), StructField("_2", IntegerType, nullable = false))), containsNull = true),
      //    typeTag[Iterable[(String, Int)]] -> ArrayType(StructType(Seq(StructField("_1", StringType), StructField("_2", IntegerType, nullable = false))), containsNull = true)
    )

    oneWayPairs.foreach {
      pair =>
        it(s"scalaType (${pair._1.tpe}) => catalystType (${pair._2})") {
          val converted = pair._1.tryReify.toOption
          println(converted)
          assert(converted == Some(pair._2))
        }
    }

    it("ScalaUDT will not interfere with catalyst CodeGen") {
      val df = TestHelper.TestSQL.createDataFrame(
        Seq(
          1 -> new Example("a", 1),
          2 -> new Example("b", 2),
          3 -> null
        )
      )
      df.filter(df.col("`_2`").isNotNull)
        .show()
    }
  }

  describe("TypeTag <=> ClassTag") {

    val ttgs = Seq(
      typeTag[Int],
      typeTag[User],
      typeTag[Multipart],
      typeTag[Multipart.type]
//      typeTag[Map[_,_]], //TODO: why this fails? Add back!
//      typeTag[List[_]]
    )

    ttgs.foreach {
      ttg =>
        it(s"${ttg.tpe}: TypeTag => mirror") {
          val mirror = (ttg: ScalaType[_]).mirror
          assert(mirror != null)
        }

        it(s"${ttg.tpe}: TypeTag => ClassTag => TypeTag") {

          val ctg = (ttg: ScalaType[_]).asClassTag
          val ttg2 = (ctg: ScalaType[_]).asTypeTag
          assert(ttg.tpe =:= ttg2.tpe)
        }

        it(s"${ttg.tpe}: TypeTag => Class => TypeTag") {
          val clz = (ttg: ScalaType[_]).asClass
          val ttg2 = (clz: ScalaType[_]).asTypeTag
          assert(ttg.tpe =:= ttg2.tpe)
        }
    }
  }
}
