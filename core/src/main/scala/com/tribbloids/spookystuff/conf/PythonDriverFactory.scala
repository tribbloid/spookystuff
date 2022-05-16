package com.tribbloids.spookystuff.conf

import com.tribbloids.spookystuff.SpookyContext
import com.tribbloids.spookystuff.session.{PythonDriver, Session}
import com.tribbloids.spookystuff.utils.lifespan.Cleanable.Lifespan

case class PythonDriverFactory(
    getExecutable: SpookyContext => String
) extends DriverFactory.Transient[PythonDriver] {

  override def factoryReset(driver: PythonDriver): Unit = {}

  override def _createImpl(session: Session, lifespan: Lifespan): PythonDriver = {
    val exeStr = getExecutable(session.spooky)
    new PythonDriver(exeStr, _lifespan = lifespan)
  }
}

object PythonDriverFactory {

  object _3 extends PythonDriverFactory((_: SpookyContext) => "python3")
}
