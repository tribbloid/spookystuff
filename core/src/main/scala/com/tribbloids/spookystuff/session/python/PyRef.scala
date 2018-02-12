
package com.tribbloids.spookystuff.session.python

import com.tribbloids.spookystuff.SpookyContext
import com.tribbloids.spookystuff.session._
import com.tribbloids.spookystuff.utils.CachingUtils.ConcurrentMap
import com.tribbloids.spookystuff.utils.lifespan.{Cleanable, Lifespan, LocalCleanable}
import org.apache.spark.ml.dsl.utils.refl.ReflectionUtils
import com.tribbloids.spookystuff.utils.SpookyUtils
import org.apache.spark.ml.dsl.utils._
import org.apache.spark.ml.dsl.utils.messaging.{MessageAPI, MessageWriter}
import org.json4s.JsonAST.JValue
import org.json4s.jackson.JsonMethods._

import scala.language.dynamics

trait PyRef extends Cleanable {

  type Binding <: PyBinding

  // the following are only used by non-singleton subclasses
  def className = this.getClass.getCanonicalName

  /**
    * assumes that a Python class is always defined under pyspookystuff.
    */
  lazy val pyClassNameParts: Seq[String] = {
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

  @transient lazy val _driverToBindings: ConcurrentMap[PythonDriver, PyBinding] = {
    ConcurrentMap()
  }

  def driverToBindingsAlive: ConcurrentMap[PythonDriver, PyBinding] = {
    val deadBindings = _driverToBindings.filter(_._1.isCleaned).toSeq
    deadBindings.foreach {
      _._2.clean(true)
    }
    _driverToBindings --= deadBindings.map(_._1)
    _driverToBindings
  }
  // prevent concurrent modification error
  def bindings: List[PyBinding] = driverToBindingsAlive.values.toList

  def imports: Seq[String] = Seq(
    "import simplejson as json"
  )

  def createOpt: Option[String] = None
  def referenceOpt: Option[String] = None

  // run on each driver
  // TODO: DO NOT override this, use __del__() in python implementation as much as you can so it will be called by python interpreter shutdown hook
  def delOpt: Option[String] = if (createOpt.nonEmpty) {
    referenceOpt.map(
      v =>
        s"""
           |try:
           |  del($v)
           |except NameError:
           |  pass
           """.stripMargin
    )
  }
  else {
    None
  }

  def dependencies: Seq[PyRef] = Nil // has to be initialized before calling the constructor

  def lzy: Boolean = true //set to false to enable immediate Binding initialization

  def converter: PyConverter = PyConverter.JSON

  def pyClassName: String = pyClassNameParts.mkString(".").stripSuffix("$")
  def simpleClassName = pyClassNameParts.last
  def varNamePrefix = FlowUtils.toCamelCase(simpleClassName)
  def packageName = pyClassNameParts.slice(0, pyClassNameParts.length - 1).mkString(".")

  override def chainClean: Seq[Cleanable] = bindings

  def _Py(
           driver: PythonDriver,
           spookyOpt: Option[SpookyContext] = None
         ): Binding = {

    driverToBindingsAlive.getOrElse(
      driver,
      newPyDecorator(newPy(driver, spookyOpt))
    )
      .asInstanceOf[Binding]
  }

  protected def newPyDecorator(v: => PyBinding): PyBinding = v
  protected def newPy(driver: PythonDriver, spookyOpt: Option[SpookyContext]): PyBinding = {
    new PyBinding(this, driver, spookyOpt)
  }

  def Py(session: Session): Binding = {
    _Py(session.pythonDriver, Some(session.spooky))
  }

  override protected def cleanImpl(): Unit = {}
}

/**
  * bind to a session
  * may be necessary to register with PythonDriver shutdown listener
  */
class PyBinding (
                  val ref: PyRef,
                  val driver: PythonDriver,
                  val spookyOpt: Option[SpookyContext]
                ) extends Dynamic with LocalCleanable {

  import ref._

  {
    assertNotCleaned("cannot create binding")
    dependencies.foreach {
      dep =>
        dep._Py(driver, spookyOpt)
    }

    driver.batchImport(imports)

    val initOpt = createOpt.map {
      create =>
        (referenceOpt.toSeq ++ Seq(create)).mkString("=")
    }

    initOpt.foreach {
      code =>
        if (lzy) driver.lazyInterpret(code)
        else driver.interpret(code)
    }

    ref.driverToBindingsAlive += driver -> this
  }

  //TODO: rename to something that is illegal in python syntax
  def $STR: Option[String] = {
    referenceOpt.flatMap {
      ref =>
        driver.evalExpr(ref)
    }
  }

  def $TYPE: Option[String] = {
    ???
  }

  // TODO: so far, doesn't support nested object
  def $MSG: Option[MessageWriter[JValue]] = {

    referenceOpt.flatMap {
      ref =>
        //        val jsonOpt = driver.evalExpr(s"$ref.__dict__")
        val jsonOpt = driver.evalExpr(s"json.dumps($ref.__dict__)")
        jsonOpt.map {
          json =>
            val jValue = parse(json)
            MessageWriter(jValue)
        }
    }
  }

  private def pyCallMethod(methodName: String)(py: (Seq[PyRef], String)): PyBinding = {

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

  protected def dynamicDecorator(fn: => PyBinding): PyBinding = fn

  def selectDynamic(fieldName: String) = {
    dynamicDecorator{
      pyCallMethod(fieldName)(Nil -> "")
    }
  }
  def applyDynamic(methodName: String)(args: Any*) = {
    dynamicDecorator {
      pyCallMethod(methodName)(converter.args2Ref(args))
    }
  }
  def applyDynamicNamed(methodName: String)(kwargs: (String, Any)*) = {
    dynamicDecorator {
      pyCallMethod(methodName)(converter.kwargs2Ref(kwargs))
    }
  }

  /**
    * chain to all bindings with active drivers
    */
  override protected def cleanImpl(): Unit = {
    if (!driver.isCleaned) {
      delOpt.foreach {
        code =>
          driver.interpret(code, spookyOpt)
      }
    }

    driverToBindingsAlive.remove(this.driver)
  }
}

object PyRef {

  object ROOT extends PyRef {
  }

  def sanityCheck(): Unit = {
    val subs = Cleanable.getTyped[PyBinding]
    val refSubs = Cleanable.getTyped[PyRef].map(_.chainClean)
    assert(
      subs.intersect(refSubs).size <= refSubs.size,
      {
        "INTERNAL ERROR: dangling tree!"
      }
    )
  }
}

trait NoneRef extends PyRef {
  override final val referenceOpt = Some("None")
  override final val delOpt = None
}

case class DetachedRef(
                        override val createOpt: Option[String],
                        override val referenceOpt: Option[String],
                        override val dependencies: Seq[PyRef],
                        override val converter: PyConverter
                      ) extends PyRef {

  override def lzy = false
}

trait ClassRef extends PyRef {

  override def imports = super.imports ++ Seq(s"import $packageName")

  override lazy val referenceOpt = Some(varNamePrefix + SpookyUtils.randomSuffix)
}

trait StaticRef extends ClassRef {

  assert(
    className.endsWith("$"),
    s"$className is not an object, only object can implement PyStatic"
  )

  override lazy val createOpt = None

  override lazy val referenceOpt = Some(pyClassName)

  override lazy val delOpt = None
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

  override def createOpt = Some(
    s"""
       |$pyClassName$pyConstructorArgs
      """.trim.stripMargin
  )
}

@Deprecated
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

  override def dependencies = {
    this.converter.kwargs2Ref(attrMap)._1
  }

  override def pyConstructorArgs = {
    this.converter.kwargs2Ref(attrMap)._2
  }
}

trait SingletonRef extends PyRef {

  override protected def newPyDecorator(v: => PyBinding): PyBinding = {
    require(driverToBindingsAlive.isEmpty, "can only be bind to one driver")
    v
  }

  def PY = driverToBindingsAlive.values.head
}

trait BindedRef extends SingletonRef with LocalCleanable {

  def driverTemplate: PythonDriver

  @transient var _driver: PythonDriver = _
  def driver = this.synchronized {
    Option(_driver).getOrElse{
      val v = new PythonDriver(
        driverTemplate.pythonExe,
        driverTemplate.autoImports,
        _lifespan = new Lifespan.JVM(
          nameOpt = Some(this.getClass.getSimpleName))
      )
      _driver = v
      v
    }
  }
  override def PY: PyBinding = super._Py(driver)
  def PYOpt: Option[PyBinding] = Option(_driver).map {
    driver =>
      super._Py(driver)
  }

  def stopDriver(): Unit = {
    Option(_driver).foreach(_.clean())
    _driver = null
  }

  override def _Py(
                    driver: PythonDriver,
                    spookyOpt: Option[SpookyContext] = None
                  ): Binding = {
    throw new UnsupportedOperationException("NOT ALLOWED! use PY instead")
  }

  override protected def cleanImpl(): Unit = {
    super.cleanImpl()
    stopDriver()
  }
}