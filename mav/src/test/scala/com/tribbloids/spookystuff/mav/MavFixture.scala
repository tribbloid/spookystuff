package com.tribbloids.spookystuff.mav

import com.tribbloids.spookystuff.SpookyEnvFixture
import com.tribbloids.spookystuff.session.DriverSession

import scala.collection.mutable.ArrayBuffer

/**
  * Created by peng on 01/10/16.
  */
object MavFixture {

  //TODO: delete, already delegated to Python component
  val usedI = ArrayBuffer[Int]()

  def nextI: Int = this.synchronized {
    val res = (0 until 16).find(
      i =>
        !(usedI contains i)
    )
      .get
    usedI += res
    res
  }

  def sitlUp(session: DriverSession, i: Int = nextI): Int = {
    //    val port = Instance.sitlI2Port(i)
    session.getOrCreatePythonDriver.call(
      s"""
         |from pyspookystuff.mav import sitlUp
         |
         |sitlUp($i)
         """.stripMargin
    )
    i
  }

  def sitlClean(session: DriverSession): Unit = {
    session.getOrCreatePythonDriver.call(
      s"""
         |from pyspookystuff.mav import sitlClean
         |
         |sitlClean()
        """.stripMargin
    )
  }
}

class MavFixture extends SpookyEnvFixture {

  import com.tribbloids.spookystuff.utils.SpookyViews._

  def sitlForeachExecutor(): Unit = {
    val spooky = this.spooky
    sc.foreachExecutor {
      val session = new DriverSession(spooky)
      MavFixture.sitlUp(session)
    }
  }

  def sitlDown(): Unit = {
    val spooky = this.spooky
    sc.foreachExecutor {
      val session = new DriverSession(spooky)
      MavFixture.sitlClean(session)
    }
  }

  override def beforeAll(): Unit = {
    super.beforeAll()
    sitlForeachExecutor()
  }

  override def afterAll(): Unit = {
    sitlDown()
    super.afterAll()
  }
}
