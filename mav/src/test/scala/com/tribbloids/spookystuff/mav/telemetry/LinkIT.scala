package com.tribbloids.spookystuff.mav.telemetry

import com.tribbloids.spookystuff.SpookyContext
import com.tribbloids.spookystuff.mav.sim.APMSimFixture
import com.tribbloids.spookystuff.session.Session
import org.apache.spark.rdd.RDD

/**
  * Created by peng on 31/10/16.
  */
object LinkIT{

  def moveAndGetLocation(
                          spooky: SpookyContext,
                          proxyFactory: ProxyFactory,
                          connStr: String
                        ): String = {

    val endpoint = Endpoint(Seq(connStr))
    val session = new Session(spooky)
    val link = Link.getOrCreate(
      Seq(endpoint),
      proxyFactory,
      session
    )

    val location = link.Py(session)
      .testMove()
      .strOpt
      .get

    //    link.Py(session).disconnect()

    location
  }
}

class LinkIT extends APMSimFixture {

  lazy val proxyFactory: ProxyFactory = ProxyFactories.NoProxy

  //  override def parallelism: Int = 2

  test("move 1 drone") {
    val spooky = this.spooky
    val proxyFactory = this.proxyFactory
    val rdd = sc.parallelize(Seq(this.simConnStrs.head))
      .map {
        connStr =>
          LinkIT.moveAndGetLocation(spooky,
            proxyFactory, connStr)
      }
    val location = rdd.first()

    println(location)
    assert(spooky.metrics.linkCreated.value == 1)
  }

  test("move drones to different directions") {
    val spooky = this.spooky
    val proxyFactory = this.proxyFactory
    val rdd = simConnStrRDD.map {
      connStr =>
        LinkIT.moveAndGetLocation(spooky, proxyFactory, connStr)
    }
      .persist()
    val locations: Array[String] = try {
      val locations = rdd.collect()
      assert(locations.distinct.length == locations.length)
      locations
    }

    locations.toSeq.foreach(
      println
    )
    assert(spooky.metrics.linkCreated.value == parallelism - 1)
  }

  test("move drones to different directions several times") {
    val spooky = this.spooky
    val proxyFactory = this.proxyFactory
    var locations: Array[String] = null

    for (i <- 1 to 10) {
      val rdd: RDD[String] = simConnStrRDD.map {
        connStr =>
          LinkIT.moveAndGetLocation(spooky, proxyFactory, connStr)
      }

      locations = {
        val locations = rdd.collect()
        assert(locations.distinct.length == locations.length)
        locations
      }
    }

    locations.toSeq.foreach(
      println
    )
    assert(spooky.metrics.linkCreated.value == 0)
  }
}
