package com.tribbloids.spookystuff.testutils

import ai.acyclic.prover.commons.spark.SparkContextView
import org.scalatest.{Status, Suite}

import java.util.concurrent.atomic.AtomicInteger

trait SparkUISupport extends Suite {

  abstract override protected def runTest(testName: String, args: org.scalatest.Args): Status = {

    lazy val fullText = s"[${this.suiteName}] $testName"

    SparkContextView(TestHelper.TestSC).withJob(fullText, SparkUISupport.serID.getAndIncrement().toString) {

      super.runTest(testName, args)
    }
  }

}

object SparkUISupport {

  val serID: AtomicInteger = new AtomicInteger()
}
