package com.tribbloids.spookystuff.conf

import com.tribbloids.spookystuff.SpookyContext
import com.tribbloids.spookystuff.session.{Agent, PythonDriver}
import com.tribbloids.spookystuff.utils.lifespan.Cleanable.Lifespan

case class PythonDriverFactory(
    getExecutable: SpookyContext => String
) extends DriverFactory.Transient[PythonDriver] {

  override def factoryReset(driver: PythonDriver): Unit = {}

  override def _createImpl(agent: Agent, lifespan: Lifespan): PythonDriver = {
    val exeStr = getExecutable(agent.spooky)
    new PythonDriver(exeStr, _lifespan = lifespan)
  }
}

object PythonDriverFactory {

  lazy val python3: String = "python3"

  object _3 extends PythonDriverFactory((_: SpookyContext) => python3)
}
