package com.tribbloids.spookystuff.python.ref

import com.tribbloids.spookystuff.SpookyContext
import com.tribbloids.spookystuff.session.PythonDriver
import com.tribbloids.spookystuff.utils.lifespan.{Lifespan, LocalCleanable}

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
