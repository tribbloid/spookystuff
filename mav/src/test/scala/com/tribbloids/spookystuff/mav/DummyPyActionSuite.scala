package com.tribbloids.spookystuff.mav

import com.tribbloids.spookystuff.SpookyEnvFixture
import com.tribbloids.spookystuff.mav.actions.DummyPyAction
import org.scalatest.FunSuite

/**
  * Created by peng on 01/09/16.
  */
class DummyPyActionSuite extends SpookyEnvFixture {

  val action = DummyPyAction()

  ignore("can execute on driver") {

    val doc = action.fetch(spooky)
    doc.map{_.toString}.mkString("\n").shouldBe(

    )
  }

  test("can execute on workers") {

  }
}
