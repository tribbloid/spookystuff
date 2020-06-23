package com.tribbloids.spookystuff.utils

import java.util.concurrent.atomic.AtomicInteger

import com.tribbloids.spookystuff.testutils.TestHelper
import org.scalatest.{Status, Suite}

trait SparkUISupport extends Suite {

  protected abstract override def runTest(testName: String, args: org.scalatest.Args): Status = {

    lazy val fullText = s"[${this.suiteName}] $testName]"

    SCFunctions(TestHelper.TestSC).withJob(fullText, SparkUISupport.serID.getAndIncrement().toString) {

      super.runTest(testName, args)
    }
  }

}

object SparkUISupport {

  val serID: AtomicInteger = new AtomicInteger()
}
