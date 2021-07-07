package com.tribbloids.spookystuff.python.ref

import com.tribbloids.spookystuff.SpookyContext
import com.tribbloids.spookystuff.python.PyConverter
import com.tribbloids.spookystuff.session._
import com.tribbloids.spookystuff.utils.CachingUtils.ConcurrentMap
import com.tribbloids.spookystuff.utils.lifespan.Cleanable
import org.apache.spark.ml.dsl.utils._

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
    ).split('.')
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
  def delOpt: Option[String] =
    if (createOpt.nonEmpty) {
      referenceOpt.map(
        v => s"""
           |try:
           |  del($v)
           |except NameError:
           |  pass
           """.stripMargin
      )
    } else {
      None
    }

  def dependencies: Seq[PyRef] = Nil // has to be initialized before calling the constructor

  def lzy: Boolean = true //set to false to enable immediate Binding initialization

  def converter: PyConverter = PyConverter.JSON

  def pyClassName: String = pyClassNameParts.mkString(".").stripSuffix("$")
  def simpleClassName = pyClassNameParts.last
  def varNamePrefix = DSLUtils.toCamelCase(simpleClassName)
  def packageName = pyClassNameParts.slice(0, pyClassNameParts.length - 1).mkString(".")

  override def chainClean: Seq[Cleanable] = bindings

  def _Py(
      driver: PythonDriver,
      spookyOpt: Option[SpookyContext] = None
  ): Binding = {

    driverToBindingsAlive
      .getOrElse(
        driver,
        newPy(driver, spookyOpt)
      )
      .asInstanceOf[Binding]
  }

  protected def newPy(driver: PythonDriver, spookyOpt: Option[SpookyContext]): PyBinding = {
    new PyBinding(this, driver, spookyOpt)
  }

  def Py(session: Session): Binding = {
    _Py(session.pythonDriver, Some(session.spooky))
  }

  override protected def cleanImpl(): Unit = {}
}

object PyRef {

  object ROOT extends PyRef {}

  def sanityCheck(): Unit = {
    val subs = Cleanable.getTyped[PyBinding]
    val refSubs = Cleanable.getTyped[PyRef].map(_.chainClean)
    assert(
      subs.intersect(refSubs).size <= refSubs.size, {
        "INTERNAL ERROR: dangling tree!"
      }
    )
  }
}
