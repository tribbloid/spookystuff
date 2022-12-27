package com.tribbloids.spookystuff

import com.tribbloids.spookystuff.utils.serialization.{AssertSerializable, NOTSerializable}
import org.apache.spark.ml.dsl.utils.refl.ScalaUDT
import org.apache.spark.sql.types.SQLUserDefinedType
import org.scalatest.Assertions

object TestBeans {

  class GenericExample[T](
      val a: String,
      val b: T
  ) extends Serializable {

    lazy val c: String = a + b

    def fn: T = b
    def fn(i: T): String = "" + b + i
    def fnBlock(x: T)(y: T, z: T): String = "" + b + x + y + z

    def fnOpt(x: T): Option[T] = {
      if (x == null) None
      else if (b.hashCode() >= x.hashCode()) Some(x)
      else None
    }
    def fnOptOpt(x: Option[T], default: T): Option[T] = {
      fnOpt(x.getOrElse(default))
    }

    def fnDefault(
        a: T,
        b: String = "b"
    ): String = "" + a + b

    def *=>(k: T): String = "" + k
  }

  class ExampleUDT extends ScalaUDT[Example]
  @SQLUserDefinedType(udt = classOf[ExampleUDT])
  class Example(
      override val a: String = "dummy",
      override val b: Int = 1
  ) extends GenericExample[Int](a, b)

  case class WithID(_id: Int) {
    override def toString: String = _id.toString
  }

  class NOTSerializableID(_id: Int) extends WithID(_id) with NOTSerializable {

    Assertions.intercept[Exception] {
      AssertSerializable(this)
    }
  }
  object NOTSerializableID {
    def apply(_id: Int): NOTSerializableID = new NOTSerializableID(_id)
  }

}
