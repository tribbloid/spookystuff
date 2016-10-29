
package com.tribbloids.spookystuff.actions

import com.tribbloids.spookystuff.doc.Fetched
import com.tribbloids.spookystuff.session.{Cleanable, DriverSession, PythonDriver, Session}
import com.tribbloids.spookystuff.{SpookyContext, caching}
import org.apache.spark.ml.dsl.utils._

import scala.language.dynamics
import scala.util.Random

trait PyConverter {

  def scala2py(v: Any): (String, String) //import -> definition
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
    def scala2py(v: Any): (String, String) = {
      v match {
        case vv: PyObject =>
          val pyFullName = vv.pyClassNames.mkString(".")
          val pyPackage = vv.pyClassNames.slice(0, vv.pyClassNames.length - 1).mkString(".")
          //          val pyClass = pyFullName.last
          val json = vv.toMessage.prettyJSON
          val impt = s"import $pyPackage"
          val code =
            s"""
               |$pyFullName(**(json.loads(
               |$QQQ
               |$json
               |$QQQ
               |)))
               """.trim.stripMargin
          impt -> code
        case _ =>
          val json = MessageView(v).prettyJSON
          val code =
            s"""
               |json.loads(
               |$QQQ
               |$json
               |$QQQ
               |)
               """.trim.stripMargin
          "" -> code
      }
    }

    def args2py(vs: Iterable[Any]): (String, String) = {
      val pys = vs.map {
        v =>
          scala2py(v)
      }
      val impt = pys.map(_._1).mkString("\n")
      val code = pys.map(_._2).mkString(",")
      impt -> code
    }

    def kwargs2py(vs: Iterable[(String, Any)]): (String, String) = {
      val pys = vs.map {
        tuple =>
          val py = scala2py(tuple._2)
          py._1 -> (tuple._1 + "=" + py._2)
      }

      val impt = pys.map(_._1).mkString("\n")
      val code = pys.map(_._2).mkString(",")
      impt -> code
    }
  }
}

/**
  * bind to a session
  * may be necessary to register with PythonDriver shutdown listener
  */
trait PyBinding extends Dynamic {

  def driver: PythonDriver
  def spookyOpt: Option[SpookyContext]

  def prefix: String

  def converter = PyConverter.JSON

  def getTempVarName = "var" + Math.abs(Random.nextLong())

  def applyDynamic(methodName: String)(args: Any*): Option[String] = {

    pyApply(methodName)(converter.args2py(args))
  }

  def pyApply(methodName: String)(py: (String, String)): Option[String] = {

    val tempVarName = getTempVarName
    val code =
      s"""
         |${py._1}
         |$tempVarName=$prefix$methodName(
         |${py._2}
         |)
      """.trim.stripMargin
    val result = driver.execute(code, Some(tempVarName), spookyOpt = spookyOpt)._2

    result
  }

  def applyDynamicNamed(methodName: String)(kwargs: (String, Any)*): Option[String] = {

    pyApply(methodName)(converter.kwargs2py(kwargs))
  }
}

case class RootBinding(
                        override val driver: PythonDriver,
                        override val spookyOpt: Option[SpookyContext]
                      ) extends PyBinding {

  override def prefix: String = ""
}

/**
  * NOT thread safe!
  */
trait PyObject extends HasMessage with Cleanable {

  /**
    * assumes that a Python class is always defined under pyspookystuff.
    */
  val pyClassNames: Array[String] = (
    "py" +
      this.getClass.getCanonicalName
        .split('.')
        .slice(2, Int.MaxValue)
        .filter(_.nonEmpty)
        .mkString(".")
    )
    .split('.')

  def converter = PyConverter.JSON

  val varPrefix = FlowUtils.toCamelCase(this.getClass.getSimpleName)

  @transient lazy val driverToBindings: caching.ConcurrentMap[PythonDriver, Binding] = caching.ConcurrentMap()
  def bindings = driverToBindings.values.toList

  protected def _clean() = {
    bindings.foreach {
      _.finalize()
    }
  }

  def Py(session: Session): Binding = {
    session.asInstanceOf[DriverSession].initializeDriverIfMissing {
      driverToBindings.getOrElse(
        session.pythonDriver,
        Binding(session.pythonDriver, Some(session.spooky))
      )
    }
  }

  case class Binding(
                      override val driver: PythonDriver,
                      override val spookyOpt: Option[SpookyContext]
                    ) extends PyBinding with Cleanable {

    driverToBindings += driver -> this

    @transient var varNameOpt: Option[String] = None

    //lazy construction
    def varName: String = varNameOpt.getOrElse {
      val varName = _init()
      varName
    }

    def prefix = varName + "."

    //Python class must have a constructor that takes json format of its Scala class
    protected def _init(): String = {

      val varName = varPrefix + "_" + System.identityHashCode(this)
      val py = converter.scala2py(PyObject.this)
      val code =
        s"""
           |${py._1}
           |$varName = ${py._2}
       """.trim.stripMargin

      driver.interpret(code, spookyOpt)
      this.varNameOpt = Some(varName)
      varName
    }

    //TODO: register interpreter shutdown hook listener?
    protected override def _clean(): Unit = {
      varNameOpt.foreach {
        vn =>
          driver.interpret(
            s"del $vn",
            spookyOpt
          )
          this.varNameOpt = None
      }

      PyObject.this.driverToBindings.get(this.driver)
        .foreach {
          v =>
            if (v == this)
              PyObject.this.driverToBindings - this.driver
        }
    }
  }
}

trait PyAction extends Action with PyObject {

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
        this.Py(session).finalize()
      }
    }
  }
}