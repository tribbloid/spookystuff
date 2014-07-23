package org.tribbloid.spookystuff.spike

import org.apache.spark.deploy.SparkSubmit

/**
 * Created by peng on 21/07/14.
 */
object TestSparkSubmit {

  def main(args: Array[String]) {
    SparkSubmit.main(
      Array(
        "--master", "local[*]",
        "--class", "org.tribbloid.spookystuff.example.LinkedInSimple",
        "example/target/spookystuff-example-assembly-0.1.0-SNAPSHOT.jar"
      )
    )
  }
}
