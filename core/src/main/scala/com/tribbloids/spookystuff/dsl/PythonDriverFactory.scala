package com.tribbloids.spookystuff.dsl

import com.tribbloids.spookystuff.SpookyContext
import com.tribbloids.spookystuff.session.{PythonDriver, Session}
import com.tribbloids.spookystuff.utils.lifespan.Lifespan

abstract class PythonDriverFactory extends DriverFactory.Transient[PythonDriver] {

  override def factoryReset(driver: PythonDriver): Unit = {}
}

object PythonDriverFactory {

  case class Python(
      getExecutable: SpookyContext => String
  ) extends PythonDriverFactory {

    override def _createImpl(session: Session, lifespan: Lifespan): PythonDriver = {
      val exeStr = getExecutable(session.spooky)
      new PythonDriver(exeStr, _lifespan = lifespan)
    }
  }

  object Python3 extends Python((_: SpookyContext) => "python3")
}
