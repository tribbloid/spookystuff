//package org.tribbloid.spookystuff.sparkbinding
//
//import org.apache.spark.rdd.RDD
//
//
///**
// * Created by peng on 09/06/14.
// */
//class RetryingRDDFunctions[U ](val self: RDD[U]) {
//
//}
//
//object Retry {
//
//  /**
//   * retry a particular block that can fail
//   *
//   * @param maxRetry  how many times to retry before to giveup
//   * @param block	a block of code to retry
//   * @returns	 value computed
//   * @throws last exception thrown by fn
//   */
//  def retry[T](maxRetry: Int = 3, useFailedResult: Boolean = false, failedResult: T = null)(block: => T): T = {
//
//    try {
//      block
//    } catch {
//      case e: Throwable =>
//        if (maxRetry > 1) retry(maxRetry - 1, useFailedResult, failedResult)(block)
//        else if (useFailedResult == true) failedResult
//        else throw e
//    }
//  }
//
//}