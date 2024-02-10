package com.tribbloids.spookystuff.utils.refl

import com.tribbloids.spookystuff.testutils.BaseSpec
import com.tribbloids.spookystuff.utils.refl.TypeUtils
import com.tribbloids.spookystuff.utils.serialization.AssertSerializable
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.catalyst.ScalaReflection.universe
import org.scalatest.Ignore

import scala.reflect.ClassTag

@Ignore
class TypeMagnetSpike extends BaseSpec {

  import ScalaReflection.universe._

  /**
    * please keep this test to quickly identify any potential problems caused by changes in scala reflection API in the
    * future
    */
  it("scala reflection can be used to get type of Array[String].headOption") {

    val arr: Seq[String] = Seq("abc", "def")
    val cls = arr.head.getClass
    val ttg: TypeTag[Seq[String]] = TypeUtils.summon(arr)
    val fns = ttg.tpe.members
    val fn: universe.Symbol = fns
      .filter(_.name.toString == "head")
      .head // Unsafely access it for now, use Option and map under normal conditions

    val fnTp: Type = fn.typeSignatureIn(ttg.tpe)

    val clsTp = fnTp.typeSymbol.asClass
    val fnRetTp = fnTp.asInstanceOf[ScalaReflection.universe.NullaryMethodType].resultType

    val fnCls = ttg.mirror.runtimeClass(fnRetTp)
    val fnCls2 = ttg.mirror.runtimeClass(clsTp)

    assert(cls == fnCls)
    assert(cls == fnCls2)
  }

  // this will definitely break
  ignore("can reflect lambda") {

    val ll = { v: String =>
      v.toInt
    }

    val llClass = {

      val clazz = ll.getClass

      println(clazz.getSuperclass)
      println("---")
      clazz.getInterfaces.foreach(println)
      clazz
    }
    val mirror = runtimeMirror(llClass.getClassLoader)

    val sym = mirror.classSymbol(llClass)

    print(sym.selfType)
  }

  ignore("TypeTag from Type can be serializable") {

    val ttg = implicitly[TypeTag[String]]
    val tpe = ttg.tpe

    val ttgSlow = TypeUtils.createTypeTag_fast(tpe, ttg.mirror)
    TypeUtils.createTypeTag_fast(tpe, ttg.mirror)

    AssertSerializable(ttgSlow)
  }

  it("can get TypeTag") {

    val ll = { v: String =>
      v.toInt
    }

    def sniff[T](v: T)(
        implicit
        ev: TypeTag[T]
    ) = ev.tpe.toString

    val ss = sniff(ll)

    print(ss)
  }

  it("can get another TypeTag") {

    val ll = new AnyRef with Function1[String, Int] with Serializable {

      override def apply(v: String) = v.toInt
    }

    def sniff[T](v: T)(
        implicit
        ev: TypeTag[T]
    ) = ev.tpe.toString

    val ss = sniff(ll)

    print(ss)
  }

  it("can reflect anon class") {

    val ll = new AnyRef with Function1[String, Int] with Serializable {

      override def apply(v: String) = v.toInt
    }

    val clazz = ll.getClass
    val mirror = runtimeMirror(clazz.getClassLoader)

    val sym = mirror.classSymbol(clazz)

    print(sym.selfType)
  }

  it("can create ClassTag for Array[T]") {

    val t = implicitly[ClassTag[String]]
    val ts = implicitly[ClassTag[Array[String]]]

    Seq(t, ts).foreach(println)

  }
}
