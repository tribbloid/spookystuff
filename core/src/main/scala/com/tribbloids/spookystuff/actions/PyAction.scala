
package com.tribbloids.spookystuff.actions

import com.tribbloids.spookystuff.doc.Fetched
import com.tribbloids.spookystuff.session.{Cleanable, DriverSession, PythonDriver, Session}
import com.tribbloids.spookystuff.utils.SpookyUtils
import com.tribbloids.spookystuff.{SpookyContext, caching}
import org.apache.spark.ml.dsl.utils._

import scala.language.dynamics

trait PyConverter {

  def scala2py(v: Any): (Seq[PyRef], String) //preprocessing -> definition

  def scala2ref(v: Any): (Seq[PyRef], String) = {
    v match {
      case vv: PyRef =>
        vv.dependencies -> vv.referenceOpt.get
      case _ =>
        scala2py(v)
    }
  }

  def args2Ref(vs: Iterable[Any]): (Seq[PyRef], String) = {
    val pys = vs.map {
      v =>
        scala2ref(v)
    }
    val deps = pys.map(_._1).reduce(_ ++ _)
    val code =
      s"""
         |(
         |${pys.map(_._2).mkString(",")}
         |)
         """.trim.stripMargin
    deps -> code
  }

  def kwargs2Ref(vs: Iterable[(String, Any)]): (Seq[PyRef], String) = {
    val pys = vs.map {
      tuple =>
        val py = scala2ref(tuple._2)
        py._1 -> (tuple._1 + "=" + py._2)
    }

    val deps = pys.map(_._1).reduce(_ ++ _)
    val code =
      s"""
         |(
         |${pys.map(_._2).mkString(",")}
         |)
         """.trim.stripMargin
    deps -> code
  }
}

object PyConverter {

  final val QQQ = "\"\"\""

  /**
    * convert scala object to python sourcecode
    * case class is convert to Python Constructor or variable
    * other types => JSON => Python types
    */
  object JSON extends PyConverter {

    // How deep does the recursion goes?
    // as deep as not inspecting case class constructor, if you do it all hell break loose
    // this limits class constructor to use only JSON compatible weak types, which is not a big deal.
    def scala2py(v: Any): (Seq[PyRef], String) = {
      val json = v match {
        case vv: HasMessage =>
          vv.toMessage.prettyJSON
        case _ =>
          MessageView(v).prettyJSON
      }
      val code =
        s"""
           |json.loads(
           |$QQQ
           |$json
           |$QQQ
           |)
               """.trim.stripMargin
      Nil -> code
    }
  }
}

trait PyRef extends Cleanable {

  def importOpt: Option[String] = None

  def createOpt: Option[String] = None
  def referenceOpt: Option[String] = None

  def dependencies: Seq[PyRef] = Nil // has to be initialized before calling the constructor

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

  @transient lazy val driverToBindings: caching.ConcurrentMap[PythonDriver, PyBinding] = {
    caching.ConcurrentMap()
  }
  def bindings = driverToBindings.values.toList

  protected def _clean() = {
    bindings.foreach {
      _.finalize()
    }
  }

  def Py(
          driver: PythonDriver,
          spookyOpt: Option[SpookyContext] = None
        ): PyBinding = {
    driverToBindings.getOrElse(
      driver,
      PyBinding(driver, spookyOpt)
    )
  }

  def sessionPy(session: Session): PyBinding = {
    session.asInstanceOf[DriverSession].initializeDriverIfMissing {
      driverToBindings.getOrElse(
        session.pythonDriver,
        PyBinding(session.pythonDriver, Some(session.spooky))
      )
    }
  }

  /**
    * bind to a session
    * may be necessary to register with PythonDriver shutdown listener
    */
  case class PyBinding private[PyRef](
                                       driver: PythonDriver,
                                       spookyOpt: Option[SpookyContext]
                                     ) extends Dynamic with Cleanable {

    {
      dependencies.foreach {
        dep =>
          dep.Py(driver, spookyOpt)
      }

      val initOpt = createOpt.map {
        create =>
          (referenceOpt.toSeq ++ Seq(create)).mkString("=")
      }

      val preprocessingCodes = importOpt.toSeq ++ initOpt.toSeq

      if (preprocessingCodes.nonEmpty) {

        val code = preprocessingCodes.mkString("\n")
        driver.interpret(code)
      }

      PyRef.this.driverToBindings += driver -> this
    }

    val needCleanup = createOpt.nonEmpty && referenceOpt.nonEmpty

    def pyCallMethod(methodName: String)(py: (Seq[PyRef], String)): PyRef#PyBinding = {

      val refName = methodName + SpookyUtils.randomSuffix
      val callPrefix: String = referenceOpt.map(v => v + ".").getOrElse("")

      val result = DetachedRef(
        createOpt = Some(s"$callPrefix$methodName${py._2}"),
        referenceOpt = Some(refName),
        dependencies = py._1,
        converter = converter
      )
        .Py(
          driver,
          spookyOpt
        )

      result
    }

    def toStringOpt: Option[String] = {
      referenceOpt.flatMap {
        ref =>
          val tempName = "_temp" + SpookyUtils.randomSuffix
          val result = driver.call(
            s"""
               |$tempName=$ref
            """.trim.stripMargin,
            Some(tempName),
            spookyOpt
          )
          result._2
      }
    }

    override def toString = toStringOpt.getOrElse("[No Value]")

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
                      ) extends PyRef


trait ObjectRef extends PyRef {

  override def importOpt = Some(s"import $packageName")

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

  val attrMap = SpookyUtils.Reflection.getCaseAccessorMap(this)

  override lazy val (dependencies, constructorArgsPy) = {

    val tuple: (Seq[PyRef], String) = this.converter.kwargs2Ref(attrMap)
    tuple
  }
}

trait PyAction extends Action with MessageInstanceRef {

  /**
    * must have exactly the same class/function under the same package imported in python that takes 2 JSON strings
    * 1 for action, 1 for session
    */
  final override def exe(session: Session): Seq[Fetched] = {
    withLazyDrivers(session){
      try {
        doExe(session)
      }
      finally {
        this.sessionPy(session).finalize()
      }
    }
  }
}