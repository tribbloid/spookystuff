
package com.tribbloids.spookystuff.session.python

import com.tribbloids.spookystuff.session._
import com.tribbloids.spookystuff.utils.{NOTSerializable, SpookyUtils}
import com.tribbloids.spookystuff.{SpookyContext, caching}
import org.apache.spark.ml.dsl.utils._

import scala.language.dynamics

trait PyRef extends Cleanable {

  def imports: Seq[String] = Seq(
    "import simplejson as json"
  )

  def createOpt: Option[String] = None
  def referenceOpt: Option[String] = None

  def dependencies: Seq[PyRef] = Nil // has to be initialized before calling the constructor

  def lzy: Boolean = true //set to false to enable immediate PyBinding initialization

  def converter: PyConverter = PyConverter.JSON

  // the following are only used by non-singleton subclasses
  def className = this.getClass.getCanonicalName

  /**
    * assumes that a Python class is always defined under pyspookystuff.
    */
  lazy val pyClassNames: Seq[String] = {
    (
      "py" +
        className
          .split('.')
          .slice(2, Int.MaxValue)
          .filter(_.nonEmpty)
          .mkString(".")
      )
      .split('.')
  }

  def pyClassName: String = pyClassNames.mkString(".")
  def simpleClassName = pyClassNames.last
  def varNamePrefix = FlowUtils.toCamelCase(simpleClassName)
  def packageName = pyClassNames.slice(0, pyClassNames.length - 1).mkString(".")

  @transient lazy val _driverToBindings: caching.ConcurrentMap[PythonDriver, PyBinding] = {
    caching.ConcurrentMap()
  }
  def driverToBindings = {
    val badDrivers = _driverToBindings.keys.filter(_.isCleaned)
    _driverToBindings --= badDrivers
    _driverToBindings
  }

  def bindings = driverToBindings.values.toList

  protected def _clean() = {
    bindings.foreach {
      _.finalize()
    }
  }

  def _Py(
           driver: PythonDriver,
           spookyOpt: Option[SpookyContext] = None
         ): PyBinding = {
    driverToBindings.getOrElse(
      driver,
      PyBinding(driver, spookyOpt)
    )
  }

  def Py(session: Session): PyBinding = {
    session.asInstanceOf[DriverSession].initializeDriverIfMissing {
      _Py(session.pythonDriver, Some(session.spooky))
    }
  }

  /**
    * bind to a session
    * may be necessary to register with PythonDriver shutdown listener
    */
  case class PyBinding private[PyRef](
                                       driver: PythonDriver,
                                       spookyOpt: Option[SpookyContext]
                                     ) extends Dynamic with Cleanable with NOTSerializable {

    {
      dependencies.foreach {
        dep =>
          dep._Py(driver, spookyOpt)
      }

      driver.lazyImport(imports)

      val initOpt = createOpt.map {
        create =>
          (referenceOpt.toSeq ++ Seq(create)).mkString("=")
      }

      initOpt.foreach {
        code =>
          if (lzy) driver.lazyInterpret(code)
          else driver.interpret(code)
      }

      PyRef.this.driverToBindings += driver -> this
    }

    val needCleanup = createOpt.nonEmpty && referenceOpt.nonEmpty


    //    def exe(code: String => String): Unit = {
    //      val cc = code(referenceOpt.getOrElse(""))
    //      driver.eval(
    //        cc
    //      )
    //
    //    }

    def valueOpt: Option[String] = {
      referenceOpt.flatMap {
        ref =>
          val tempName = "_temp" + SpookyUtils.randomSuffix
          val result = driver.eval(
            s"""
               |$tempName=$ref
            """.trim.stripMargin,
            Some(tempName),
            spookyOpt
          )
          result._2
      }
    }

    def value: String = valueOpt.getOrElse("[No Value]")

    def pyCallMethod(methodName: String)(py: (Seq[PyRef], String)): PyRef#PyBinding = {

      val refName = methodName + SpookyUtils.randomSuffix
      val callPrefix: String = referenceOpt.map(v => v + ".").getOrElse("")

      val result = DetachedRef(
        createOpt = Some(s"$callPrefix$methodName${py._2}"),
        referenceOpt = Some(refName),
        dependencies = py._1,
        converter = converter
      )
        ._Py(
          driver,
          spookyOpt
        )

      result
    }

    def selectDynamic(fieldName: String) = {
      pyCallMethod(fieldName)(Nil -> "")
    }
    def applyDynamic(methodName: String)(args: Any*) = {
      pyCallMethod(methodName)(converter.args2Ref(args))
    }
    def applyDynamicNamed(methodName: String)(kwargs: (String, Any)*) = {
      pyCallMethod(methodName)(converter.kwargs2Ref(kwargs))
    }

    //TODO: register interpreter shutdown hook listener?
    override protected def _clean(): Unit = {
      if (needCleanup && !driver.isCleaned) {
        referenceOpt.foreach {
          varName =>
            driver.interpret(
              s"del($varName)",
              spookyOpt
            )
        }
      }

      driverToBindings.get(this.driver)
        .foreach {
          v =>
            if (v == this)
              driverToBindings - this.driver
        }
    }
  }
}

object RootRef extends PyRef

case class DetachedRef(
                        override val createOpt: Option[String],
                        override val referenceOpt: Option[String],
                        override val dependencies: Seq[PyRef],
                        override val converter: PyConverter
                      ) extends PyRef {

  override def lzy = false
}


trait ObjectRef extends PyRef {

  override def imports = super.imports ++ Seq(s"import $packageName")

  override lazy val referenceOpt = Some(varNamePrefix + SpookyUtils.randomSuffix)
}

trait StaticRef extends ObjectRef {

  assert(
    className.endsWith("$"),
    s"$className is not an object, only object can implement PyStatic"
  )

  override lazy val pyClassName: String = super.pyClassName.stripSuffix("$")
}

/**
  * NOT thread safe!
  */
trait InstanceRef extends ObjectRef {

  assert(
    !className.contains("$"),
    s"$className is an object/anonymous class, it cannot implement PyInstance"
  )
  assert(
    !className.contains("#"),
    s"$className is a nested class, it cannot implement PyInstance"
  )

  def constructorArgsPy: String

  override lazy val createOpt = Some(
    s"""
       |$pyClassName$constructorArgsPy
      """.trim.stripMargin
  )
}

trait MessageInstanceRef extends InstanceRef with HasMessage {

  override lazy val constructorArgsPy: String = {
    val converted = this.converter.scala2py(this.toMessage)._2
    val code =
      s"""
         |(**($converted))
      """.trim.stripMargin
    code
  }
}

trait CaseInstanceRef extends InstanceRef with Product {

  def attrMap = SpookyUtils.Reflection.getCaseAccessorMap(this)

  override lazy val (dependencies, constructorArgsPy) = {

    val tuple: (Seq[PyRef], String) = this.converter.kwargs2Ref(attrMap)
    tuple
  }
}
