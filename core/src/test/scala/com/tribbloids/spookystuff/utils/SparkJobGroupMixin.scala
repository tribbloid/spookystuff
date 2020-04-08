package com.tribbloids.spookystuff.utils

import java.util.concurrent.atomic.AtomicInteger

import org.scalatest.{Status, Suite}

trait SparkJobGroupMixin extends Suite {

  protected override def runTest(testName: String, args: org.scalatest.Args): Status = {

    lazy val fullText = s"[${this.getClass.getSimpleName}: $testName]"

    SCFunctions.withJob(fullText, SparkJobGroupMixin.serID.getAndIncrement().toString) {

      super.runTest(testName, args)
    }
  }

}

object SparkJobGroupMixin {

  val serID: AtomicInteger = new AtomicInteger()
}
