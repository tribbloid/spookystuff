package com.tribbloids.spookystuff.mav.sim

import com.tribbloids.spookystuff.SpookyContext
import com.tribbloids.spookystuff.session.{Lifespan, Session}
import com.tribbloids.spookystuff.testutils.SparkRunnerHelper
import org.slf4j.LoggerFactory

/**
  * Created by peng on 02/12/16.
  */
object APMSimRunner {

  def main(args: Array[String]): Unit = {
    val sql = SparkRunnerHelper.TestSQL
    val sc = sql.sparkContext
    val spooky = new SpookyContext(sql)

    val parallelism: Int = sc.defaultParallelism

    val connStrRDD = sc.parallelize(1 to parallelism)
      .map {
        i =>
          //NOT cleaned by TaskCompletionListener
          val session = new Session(spooky, new Lifespan.JVM())
          val sim = APMSim.next
          sim.Py(session).connStr.strOpt
      }
      .flatMap(v => v)
      .persist()

    val info = connStrRDD.collect().mkString("\n")
    LoggerFactory.getLogger(this.getClass).info(
      s"""
         |APM simulation(s) are up and running:
         |$info
      """.stripMargin
    )

    while(true) {
      println("zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz...")
      Thread.sleep(10000)
    }
  }
}
