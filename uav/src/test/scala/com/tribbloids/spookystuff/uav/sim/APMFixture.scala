package com.tribbloids.spookystuff.uav.sim

import com.tribbloids.spookystuff.uav.SITLFixture
import com.tribbloids.spookystuff.uav.telemetry.Link
import com.tribbloids.spookystuff.utils.lifespan.Cleanable
import org.apache.spark.rdd.RDD
import org.slf4j.LoggerFactory

trait APMFixture extends SITLFixture {

  import com.tribbloids.spookystuff.utils.SpookyViews._

  def simFactory: SimFactory

  override def _processNames = super._processNames ++ Seq("apm")

  private var _simURIRDD: RDD[String] = _

  override lazy val fleetURIs: Seq[String] = initializeFleet

  def initializeFleet: Seq[String] = {

    cleanSweep()
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
    cleanSweep()
    Thread.sleep(2000)
    // small delay added to ensure that cleanSweep
    // won't accidentally clean object created in the suite

    super.beforeAll()
    initializeFleet
  }

  override def afterAll(): Unit = {
    cleanSweep()
    Option(this._simURIRDD).foreach(_.unpersist())
    super.afterAll()
  }

  private def cleanSweep() = {
    sc.runEverywhere() {
      _ =>
        // in production Links will be cleaned up by shutdown hook

        APMFixture.cleanSweepLocally()
        Predef.assert(
          Link.registered.isEmpty,
          Link.registered.keys.mkString("\n")
        )
    }

    val isEmpty = sc.runEverywhere() {_ => APMSim.existing.isEmpty}
    assert(!isEmpty.contains(false))
  }
}

object APMFixture {

  def cleanSweepLocally() = {
    Cleanable.cleanSweepAll {
      case _: Link => true
      case _: APMSim => true
      case _ => false
    }
  }
}