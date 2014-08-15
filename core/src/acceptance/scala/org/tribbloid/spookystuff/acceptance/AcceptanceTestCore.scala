package org.tribbloid.spookystuff.acceptance

import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.FreeSpec

/**
 * Created by peng on 22/06/14.
 */
trait AcceptanceTestCore extends FreeSpec {

  lazy val appName = this.getClass.getName
  lazy val conf: SparkConf = new SparkConf().setAppName(appName)
    .setMaster("local[8,3]")
    .setSparkHome(System.getenv("SPARK_HOME"))
  lazy val sc: SparkContext = null

  val result = doMain()

  def doMain(): Array[_]

  "Query" - {
    "should print our result" in {
      result.foreach(println(_))
    }
  }
}
