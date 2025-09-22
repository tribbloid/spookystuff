package com.tribbloids.spookystuff.python.ref

import com.tribbloids.spookystuff.commons.refl.ReflectionUtils
import com.tribbloids.spookystuff.utils.SpookyUtils
import com.tribbloids.spookystuff.relay.MessageAPI

trait ClassRef extends PyRef {

  override def imports: Seq[String] = super.imports ++ Seq(s"import $packageName")

  override lazy val referenceOpt: Some[String] = Some(varNamePrefix + SpookyUtils.randomSuffix)
}

trait StaticRef extends ClassRef {

  assert(
    className.endsWith("$"),
    s"$className is not an object, only object can implement PyStatic"
  )

  override lazy val createOpt: None.type = None

  override lazy val referenceOpt: Some[String] = Some(pyClassName)

  override lazy val delOpt: None.type = None
}

/**
  * NOT thread safe!
  */
trait InstanceRef extends ClassRef {

  assert(
    !className.contains("$"),
    s"$className is an object/anonymous class, it cannot implement PyInstance"
  )
  assert(
    !className.contains("#"),
    s"$className is a nested class, it cannot implement PyInstance"
  )

  def pyConstructorArgs: String

  override def createOpt: Some[String] = Some(
    s"""
       |$pyClassName$pyConstructorArgs
      """.trim.stripMargin
  )
}

@Deprecated
//TODO: this options should be delegated to PyConverter
trait JSONInstanceRef extends InstanceRef with MessageAPI {

  override def pyConstructorArgs: String = {
    val converted = this.converter.scala2py(this)._2
    val code =
      s"""
         |(**($converted))
      """.trim.stripMargin
    code
  }
}

trait CaseInstanceRef extends InstanceRef with Product {

  def attrMap = ReflectionUtils.getCaseAccessorMap(this)
  def kwargsTuple: (Seq[PyRef], String) = this.converter.kwargs2Code(attrMap)

  override def dependencies: Seq[PyRef] = {
    kwargsTuple._1
  }

  override def pyConstructorArgs: String = {
    kwargsTuple._2
  }
}
