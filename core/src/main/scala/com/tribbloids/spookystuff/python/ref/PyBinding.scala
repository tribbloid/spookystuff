package com.tribbloids.spookystuff.python.ref

import com.tribbloids.spookystuff.SpookyContext
import com.tribbloids.spookystuff.session.PythonDriver
import com.tribbloids.spookystuff.utils.SpookyUtils
import com.tribbloids.spookystuff.utils.lifespan.LocalCleanable
import org.apache.spark.ml.dsl.utils.messaging.MessageWriter
import org.json4s.JsonAST.JValue
import org.json4s.jackson.JsonMethods.parse

import scala.language.dynamics

/**
  * bind to a session
  * may be necessary to register with PythonDriver shutdown listener
  */
class PyBinding(
    val ref: PyRef,
    val driver: PythonDriver,
    val spookyOpt: Option[SpookyContext]
) extends Dynamic
    with LocalCleanable {

  import ref._

  {
    assertNotCleaned("cannot create binding")
    dependencies.foreach { dep =>
      dep._Py(driver, spookyOpt)
    }

    driver.batchImport(imports)

    val initOpt = createOpt.map { create =>
      (referenceOpt.toSeq ++ Seq(create)).mkString("=")
    }

    initOpt.foreach { code =>
      if (lzy) driver.lazyInterpret(code)
      else driver.interpret(code)
    }

    ref.driverToBindingsAlive += driver -> this
  }

  //TODO: rename to something that is illegal in python syntax
  def $STR: Option[String] = {
    referenceOpt.flatMap { ref =>
      driver.evalExpr(ref)
    }
  }

  def $TYPE: Option[String] = {
    ???
  }

  // TODO: so far, doesn't support nested object
  def $MSG: Option[MessageWriter[JValue]] = {

    referenceOpt.flatMap { ref =>
      //        val jsonOpt = driver.evalExpr(s"$ref.__dict__")
      val jsonOpt = driver.evalExpr(s"json.dumps($ref.__dict__)")
      jsonOpt.map { json =>
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
    )._Py(
        driver,
        spookyOpt
      )

    result
  }

  protected def dynamicDecorator(fn: => PyBinding): PyBinding = fn

  def selectDynamic(fieldName: String) = {
    dynamicDecorator {
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
      pyCallMethod(methodName)(converter.kwargs2Code(kwargs))
    }
  }

  /**
    * chain to all bindings with active drivers
    */
  override protected def cleanImpl(): Unit = {
    if (!driver.isCleaned) {
      delOpt.foreach { code =>
        driver.interpret(code, spookyOpt)
      }
    }

    driverToBindingsAlive.remove(this.driver)
  }
}
