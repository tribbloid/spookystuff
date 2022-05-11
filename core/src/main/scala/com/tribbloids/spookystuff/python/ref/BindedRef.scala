package com.tribbloids.spookystuff.python.ref

import com.tribbloids.spookystuff.SpookyContext
import com.tribbloids.spookystuff.session.PythonDriver
import com.tribbloids.spookystuff.utils.lifespan.Cleanable.Lifespan
import com.tribbloids.spookystuff.utils.lifespan.LocalCleanable

trait BindedRef extends PyRef with LocalCleanable {

  def driverTemplate: PythonDriver

  @transient var _driver: PythonDriver = _
  def driver: PythonDriver = this.synchronized {
    Option(_driver).getOrElse {
      val v = new PythonDriver(
        driverTemplate.pythonExe,
        driverTemplate.autoImports,
        _lifespan = Lifespan.JVM.apply(nameOpt = Some(this.getClass.getSimpleName))
      )
      _driver = v
      v
    }
  }

  def PY: PyBinding = {
    require(driverToBindingsAlive.forall(_._1 == driver), "can only be bind to one driver")
    super._Py(driver)
  }
  def PYOpt: Option[PyBinding] = Option(_driver).map { driver =>
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
