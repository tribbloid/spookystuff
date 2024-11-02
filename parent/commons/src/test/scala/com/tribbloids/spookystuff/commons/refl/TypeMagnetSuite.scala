package com.tribbloids.spookystuff.commons.refl

import com.tribbloids.spookystuff.commons.PairwiseConversionMixin

import java.sql.Timestamp
import com.tribbloids.spookystuff.testutils.BaseSpec
import PairwiseConversionMixin.Repr
import com.tribbloids.spookystuff.commons.refl.TypeMagnetSuite.TypeTagRepr
import com.tribbloids.spookystuff.commons.serialization.AssertSerializable
import com.tribbloids.spookystuff.relay.TestBeans._
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.catalyst.ScalaReflection.universe
import org.apache.spark.sql.types._

import scala.language.implicitConversions
import scala.reflect.ClassTag

/**
  * Created by peng on 08/06/16.
  */
object TypeMagnetSuite {

  import ScalaReflection.universe._

  class TypeTagRepr(delegate: Repr[TypeTag[_]]) extends Repr[TypeTag[_]](delegate.opt, delegate.level) {

    override lazy val valueStr: String = s"${opt.map(_.tpe).getOrElse("∅")}"

    override def forallEquals(v2: universe.TypeTag[_]): Boolean = {

      this.opt match {
        case Some(v1) =>
          v1.tpe =:= v2.tpe
        case _ =>
          true
      }
    }
  }

  trait TypeTagRepr_Level1 {

    implicit def fromDelegate_level1(v: Repr[TypeTag[_]]): TypeTagRepr = new TypeTagRepr(v)
  }

  object TypeTagRepr extends TypeTagRepr_Level1 {

    implicit def fromDelegate[T](v: Repr[TypeTag[T]]): TypeTagRepr = new TypeTagRepr(v.copy())
  }

}

class TypeMagnetSuite extends BaseSpec with PairwiseConversionMixin {

  import ScalaReflection.universe._

  class Facet(
      val _catalystType: Repr[DataType] = Repr(None, -1),
      val _typeTag: TypeTagRepr = Repr[TypeTag[_]](None, -1),
      val _classTag: Repr[ClassTag[_]] = Repr(None, -1)
  ) extends PairwiseCases {

    lazy val _class: Repr[Class[_]] = _classTag.map(_.runtimeClass)

    describe(s"From ${_typeTag}") {

      _typeTag.map { ttg =>
        def vType = TypeMagnet.FromTypeTag(ttg)

        it("has a mirror") {
          vType.mirror
        }

        it("is Serializable") {

          AssertSerializable(vType)
        }

        ignore("... even if created from raw Type") {

          val tt = ttg.tpe

          val vType2 = TypeMagnet.fromType(tt, ttg.mirror)

          AssertSerializable(vType2)
        }
      }
    }

    describe(s"From ${_classTag}") {

      _classTag.map { v =>
        def vType = TypeMagnet.FromClassTag(v)

        it("has a mirror") {
          vType.mirror
        }

        it("is Serializable") {

          AssertSerializable(vType)
        }
      }
    }

    lazy val cases: Seq[PairwiseCase[_, _]] = Seq[PairwiseCase[_, _]](
//      PairwiseCase(_typeTag, Repr[Mirror](None, 0), { r: TypeTag[_] =>
//        ScalaType.fromTypeTag(r).mirror
//      }, { _: Any =>
//        ???
//      }),
      PairwiseCase(
        _typeTag,
        _class,
        { (r: TypeTag[_]) =>
          TypeMagnet.FromTypeTag(r).asClass
        },
        { (r: Class[_]) =>
          TypeMagnet.FromClass(r).asTypeTag
        }
      ),
      PairwiseCase(
        _typeTag,
        _class.map[ClassTag[_]](v => ClassTag(v)),
        { (r: TypeTag[_]) =>
          TypeMagnet.FromTypeTag(r).asClassTag
        },
        { (r: ClassTag[_]) =>
          TypeMagnet.FromClassTag(r).asTypeTag
        }
      ),
      PairwiseCase(
        _typeTag,
        _catalystType,
        { (r: TypeTag[_]) =>
          ToCatalyst(TypeMagnet.FromTypeTag(r)).asCatalystType
        },
        { (r: DataType) =>
          CatalystTypeOps(r).magnet[Any].asTypeTag
        }
      )
    ).flatMap(_.bidirCases)
  }

  object StringFacet
      extends Facet(
        Repr(Some(StringType)),
        Repr(Some(typeTag[String])),
        Repr(None)
      )

  object IntFacet
      extends Facet(
        Repr(Some(IntegerType)),
        Repr(Some(typeTag[Int])),
        Repr(None)
      )

  object DoubleFacet
      extends Facet(
        Repr(Some(DoubleType)),
        Repr(Some(typeTag[Double])),
        Repr(None)
      )

  object TimestampFacet
      extends Facet(
        Repr(Some(TimestampType)),
        Repr(Some(typeTag[Timestamp])),
        Repr(None)
      )

  object TupleFacet
      extends Facet(
        Repr(
          Some(
            StructType(
              Seq(
                StructField("_1", StringType),
                StructField("_2", IntegerType, nullable = false)
              )
            )
          ),
          2
        ),
        Repr(
          Some(
            typeTag[(String, Int)]
          )
        ),
        Repr(
          Some(
            ClassTag(classOf[(_, _)])
          ),
          1
        )
      )

  case class ArrayFacet(delegate: Facet, containsNull: Boolean = false)
      extends Facet(
        delegate._catalystType.map { v =>
          ArrayType(v, containsNull = containsNull)
        },
        delegate._typeTag.map[TypeTag[_]] {
          case vv: TypeTag[c] =>
            implicit val _vv = vv
            val result = implicitly[TypeTag[Array[c]]]
            result
        },
        delegate._classTag
          .copy(level = 1)
          .map { v =>
            val result = v.wrap
            result
          }
      )

  case class SeqFacet(delegate: Facet, containsNull: Boolean = false)
      extends Facet(
        delegate._catalystType
          .copy(level = 1)
          .map { v =>
            ArrayType(v, containsNull = containsNull)
          },
        delegate._typeTag.map[TypeTag[_]] {
          case vv: TypeTag[c] =>
            implicit val _vv = vv
            implicitly[TypeTag[Seq[c]]]
        },
        delegate._class
          .copy(level = 2)
          .map { _ =>
            ClassTag(classOf[Seq[_]])
          }
      )

  object ClassFacet
      extends Facet(
        Repr.disabled,
        Repr(Some(typeTag[User])),
        Repr(None)
      )

  object MultipartClassFacet
      extends Facet(
        Repr.disabled,
        Repr(Some(typeTag[Multipart])),
        Repr(None)
      )

  object ObjectFacet
      extends Facet(
        Repr.disabled,
        Repr(Some(typeTag[Multipart.type])),
        Repr(None)
      )

  runAll(
    StringFacet,
    IntFacet,
    DoubleFacet,
    TimestampFacet,
    TupleFacet,
    //
    ArrayFacet(StringFacet, containsNull = true),
    ArrayFacet(IntFacet),
    ArrayFacet(DoubleFacet),
    ArrayFacet(TupleFacet, containsNull = true),
    //
    SeqFacet(StringFacet, containsNull = true),
    SeqFacet(TupleFacet, containsNull = true),
    //
    ClassFacet,
    MultipartClassFacet,
    ObjectFacet
  )

//  describe("CatalystType <==> TypeTag") {
//    val catalystType_scalaType: Seq[(DataType, TypeTag[_])] = Seq(
//      StringType -> typeTag[String],
//      IntegerType -> typeTag[Int],
//      DoubleType -> typeTag[Double],
//      ArrayType(DoubleType, containsNull = false) -> typeTag[Array[Double]],
//      //    ArrayType(DoubleType, containsNull = true) -> typeTag[Array[Option[Double]]],
//      ArrayType(StringType, containsNull = true) -> typeTag[Array[String]],
//      //    ArrayType(StringType, containsNull = true) -> typeTag[Array[(String, Int)]],
//
//      //    tupleSchema -> typeTag[(Int, String)], //TODO: urge spark team to fix the bug and re-enable it
//      //    new ActionUDT -> typeTag[Action],
//      new ExampleUDT -> typeTag[Example]
//    )
//
//    catalystType_scalaType.foreach { pair =>
//      it(s"scalaType (${pair._2.tpe}) => catalystType (${pair._1})") {
//        val converted = pair._2.tryReify.toOption
//        println(converted)
//        assert(converted.contains(pair._1))
//      }
//
//      it(s"catalystType (${pair._1}) => scalaType (${pair._2.tpe})") {
//        val converted = pair._1.typeTagOpt
//        println(converted)
//        assert(converted.map(_.asClass).contains(pair._2.asClass))
//      }
//
//      //TODO: this failed on CI for UDT with unknown reason, why?
//      it(s"CodeGenerator.javaType(${pair._1})") {
//        //        val genCtx = new CodegenContext
//        pair._1 match {
//          case v: UserDefinedType[_] =>
//            println(s"UDT: ${pair._1.getClass.getCanonicalName}")
//            assert(v.sqlType == BinaryType)
//          case _ =>
//        }
//        val tt = CodeGenerator.javaType(pair._1)
//        assert(tt.toLowerCase() != "object")
//      }
//
//    //TODO: add 1 test to ensure that ScalaUDT can be used in DataFrame with codegen.
//    }
//  }

//  describe("CatalystType => TypeTag") {
//    val oneWayPairs: Seq[(TypeTag[_], DataType)] = Seq(
//      typeTag[Array[(String, Int)]] -> ArrayType(
//        StructType(Seq(StructField("_1", StringType), StructField("_2", IntegerType, nullable = false))),
//        containsNull = true),
//      typeTag[Seq[(String, Int)]] -> ArrayType(
//        StructType(Seq(StructField("_1", StringType), StructField("_2", IntegerType, nullable = false))),
//        containsNull = true)
//      //    typeTag[Set[(String, Int)]] -> ArrayType(StructType(Seq(StructField("_1", StringType), StructField("_2", IntegerType, nullable = false))), containsNull = true),
//      //    typeTag[Iterable[(String, Int)]] -> ArrayType(StructType(Seq(StructField("_1", StringType), StructField("_2", IntegerType, nullable = false))), containsNull = true)
//    )
//
//    oneWayPairs.foreach { pair =>
//      it(s"scalaType (${pair._1.tpe}) => catalystType (${pair._2})") {
//        val converted = pair._1.tryReify.toOption
//        println(converted)
//        assert(converted.contains(pair._2))
//      }
//    }
//
//    it("ScalaUDT will not interfere with catalyst CodeGen") {
//      val df = TestHelper.TestSQL.createDataFrame(
//        Seq(
//          1 -> new Example("a", 1),
//          2 -> new Example("b", 2),
//          3 -> null
//        )
//      )
//      df.filter(df.col("`_2`").isNotNull)
//        .show()
//    }
//  }

//  describe("TypeTag <==> ClassTag") {
//
//    val ttgs = Seq(
//      typeTag[Int],
//      typeTag[User],
//      typeTag[Multipart],
//      typeTag[Multipart.type]
//      //      typeTag[Map[_,_]], //TODO: why this fails? Add back!
//      //      typeTag[List[_]]
//    )
//
//    ttgs.foreach { ttg =>
//      }
//  }
}

//object ScalaTypeSuite {
//
//  sealed trait Convertible[+T]
//
//  case class From[+T](v: T) extends Convertible[T]
//  case class To[+T](v: T) extends Convertible[T]
//  case object Cycle extends Convertible[Nothing]
//  case object Unknown extends Convertible[Nothing]
//  case object NoOp extends Convertible[Nothing]
//}
