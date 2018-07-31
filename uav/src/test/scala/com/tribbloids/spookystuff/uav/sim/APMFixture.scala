package com.tribbloids.spookystuff.uav.sim

import com.tribbloids.spookystuff.uav.{SITLFixture, UAVFixture}
import org.apache.spark.rdd.RDD
import org.slf4j.LoggerFactory

trait APMFixture extends SITLFixture {

  def simFactory: SimFactory

  override def _processNames = super._processNames ++ Seq("apm")

  private var _simURIRDD: RDD[String] = _

  override lazy val fleetURIs: Seq[String] = initializeFleet

  def initializeFleet: Seq[String] = {

    UAVFixture.cleanSweep(sc)
    Thread.sleep(2000)

    val simFactory = this.simFactory
    this._simURIRDD = sc.parallelize(1 to parallelism)
      .map {
        i =>
          val sim = simFactory.getNext
          sim.PY.connStr.$STR
      }
      .flatMap(v => v)
      .persist()

    val result = _simURIRDD.collect().toSeq
    assert(result.size == result.distinct.size)
    val info = result.mkString("\n")
    LoggerFactory.getLogger(this.getClass).info(
      s"""
         |APM simulation(s) are up and running:
         |$info
      """.stripMargin
    )
    result
  }

  override def beforeAll(): Unit = {
    super.beforeAll()
    initializeFleet
  }

  override def afterAll(): Unit = {
    Option(this._simURIRDD).foreach(_.unpersist())
    super.afterAll()
  }

}
