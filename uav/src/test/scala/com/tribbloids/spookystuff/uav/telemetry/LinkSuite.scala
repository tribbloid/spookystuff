package com.tribbloids.spookystuff.uav.telemetry

import com.tribbloids.spookystuff.session.Session
import com.tribbloids.spookystuff.testutils.TestHelper
import com.tribbloids.spookystuff.uav._
import com.tribbloids.spookystuff.uav.dsl.{Fleet, Routing}
import com.tribbloids.spookystuff.uav.system.UAV
import com.tribbloids.spookystuff.uav.telemetry.mavlink.MAVLink
import com.tribbloids.spookystuff.utils.lifespan.Cleanable
import com.tribbloids.spookystuff.utils.{CommonUtils, SpookyUtils}
import com.tribbloids.spookystuff.{SpookyContext, SpookyEnvFixture}
import org.apache.spark.rdd.RDD

object LinkSuite {

  def validate(spooky: SpookyContext, linkRDD: RDD[Link]) = {

    //    val uavStatusesSeq = for (i <- 1 to 5) yield {
    //      linkRDD.map(v => v.status()).collect().toSeq
    //    }
    //
    //    val uavsSeq = uavStatusesSeq.map(_.map(_.uav)).distinct
    //
    //    assert(uavsSeq.size == 1, uavsSeq.mkString("\n"))
    //    val uavStatuses = uavStatusesSeq.head

    val uavStatuses: Seq[LinkStatus] = linkRDD.map(v => v.status()).collect().toList

    val uavs = uavStatuses.map(_.uav)
    val uris = uavs.flatMap(_.uris)

    assert(uavs.nonEmpty)

    lazy val info = "Links are incorrect:\n" + uavStatuses
      .map { status =>
        s"${status.uav}\t${status.lockStr}"
      }
      .mkString("\n")
    if (uavs.length != spooky.sparkContext.defaultParallelism ||
        uavs.length != uavs.distinct.length ||
        uris.length != uris.distinct.length) {
      throw new AssertionError(info)
    }

    val uri_statuses = uavStatuses.flatMap { status =>
      status.uav.uris.map { uri =>
        uri -> status
      }
    }
    val grouped = uri_statuses.groupBy(_._1)
    grouped.values.foreach { v =>
      assert(
        v.length == 1,
        s""""
             |multiple UAVs sharing the same uris:
             |${v
             .map { vv =>
               s"${vv._2.uav} @ ${vv._2.lockStr}"
             }
             .mkString("\n")}
             """.stripMargin
      )
    }
  }
}

trait LinkSuite extends UAVFixture {

  def onHold = true

  import com.tribbloids.spookystuff.utils.SpookyViews._

  override def setUp(): Unit = {
    super.setUp()
    LinkUtils.cleanAll(sc)
    Thread.sleep(2000)
    // Waiting for both python drivers to terminate.
    // DON'T DELETE! some tests create proxy processes and they all take a few seconds to release the port binding!
  }

  /**
    * can only use once per test: before existing registered links are unlocked in setUp()
    * @return
    */
  def getLinkRDD(spooky: SpookyContext) = {

    val trial = LinkUtils.tryLinkRDD(spooky, onHold = onHold)

    val result: RDD[Link] = trial.map { opt =>
      val v = opt.get
      v
    }

    LinkSuite.validate(spooky, result)
    result
  }

  def routings: Seq[Routing]

  private def factory2Fixtures(factory: Routing): (SpookyContext, String) = {

    val spooky = this.spooky.copy(_configurations = this.spooky.configurations.transform(_.clone))
    val uavConf = spooky.getConf[UAVConf]
    uavConf.routing = factory
    uavConf.fleet = Fleet.Inventory(fleet)
    spooky.rebroadcast()

    val name = spooky.getConf[UAVConf].routing.getClass.getSimpleName
    spooky -> s"routing=$name"
  }

  def runTests(factories: Seq[Routing])(f: SpookyContext => Unit) = {

    val fixtures: Seq[(SpookyContext, String)] = factories.map {
      factory2Fixtures
    }

    fixtures.foreach {
      case (spooky, testPrefix) =>
        describe(testPrefix) {
          f(spooky)
        }
    }
  }

  runTests(routings) { spooky =>
    it("sanity tests") {

      val linkRDD = getLinkRDD(spooky)

      val uavss = for (i <- 0 to 10) yield {
        val uavs = linkRDD.map(_.uav).collect().toSeq
        println(s"=== $i\t===: " + uavs.mkString("\t"))

        //should be registered in both Link and Cleanable
        spooky.sparkContext.runEverywhere() { _ =>
          val registered = Link.registered.values.toSet
          val cleanable = Cleanable.getTyped[Link].toSet
          Predef.assert(registered.subsetOf(cleanable))
        }

        //Link should use different UAVs
        val uris = uavs.map(_.primaryURI)
        Predef.assert(uris.size == this.parallelism, "Duplicated URIs:\n" + uris.mkString("\n"))
        Predef.assert(uris.size == uris.distinct.size, "Duplicated URIs:\n" + uris.mkString("\n"))

        uavs
      }

      assert(uavss.distinct.size == 1, "getLinkRDD() is not idemponent" + uavss.mkString("\n"))
    }

    it("Link created in the same Task should be reused") {

      val fleet = this.fleet
      val linkStrs: Array[Boolean] = sc
        .parallelize(fleetURIs)
        .map { connStr =>
          val session = new Session(spooky)
          val link1 = Dispatcher(
            fleet,
            session
          ).get
          val link2 = Dispatcher(
            fleet,
            session
          ).get
          Thread.sleep(5000) //otherwise a task will complete so fast such that another task hasn't start yet.
          val result = link1 == link2
          result
        }
        .collect()
      assert(spooky.getMetrics[UAVMetrics].linkCreated.value == parallelism)
      assert(spooky.getMetrics[UAVMetrics].linkDestroyed.value == 0)
      linkStrs.foreach { Predef.assert }
    }

    for (routing2 <- routings) {

      //TODO Recurring error in DispatcherSuite! need inspection
      it(
        s"~> ${routing2.getClass.getSimpleName}:" +
          s" available Link can be recommissioned in another Task"
      ) {

        val routing1 = spooky.getConf[UAVConf].routing

        val linkRDD1: RDD[Link] = getLinkRDD(spooky)

        spooky.getConf[UAVConf].routing = routing2
        spooky.rebroadcast()

        try {

          assert(spooky.getMetrics[UAVMetrics].linkCreated.value == parallelism)
          assert(spooky.getMetrics[UAVMetrics].linkDestroyed.value == 0)

          LinkUtils.unlockAll(sc)

          val linkRDD2: RDD[Link] = getLinkRDD(spooky)

          if (routing1 == routing2) {
            assert(spooky.getMetrics[UAVMetrics].linkCreated.value == parallelism)
            assert(spooky.getMetrics[UAVMetrics].linkDestroyed.value == 0)
            linkRDD1
              .map(_.toString)
              .collect()
              .mkString("\n")
              .shouldBe(
                linkRDD2.map(_.toString).collect().mkString("\n"),
                sort = true
              )
          } else {
            assert(spooky.getMetrics[UAVMetrics].linkCreated.value == parallelism)
            // TODO: should be parallelism*2!
            assert(spooky.getMetrics[UAVMetrics].linkDestroyed.value == 0)
            linkRDD1
              .map(_.uav)
              .collect()
              .mkString("\n")
              .shouldBe(
                linkRDD2.map(_.uav).collect().mkString("\n"),
                sort = true
              )
          }
        } finally {
          spooky.getConf[UAVConf].routing = routing1
          spooky.rebroadcast()
        }
      }
    }
  }
}

abstract class SimLinkSuite extends SITLFixture with LinkSuite {

  import com.tribbloids.spookystuff.utils.SpookyViews._

  runTests(routings) { spooky =>
    it("Link to unreachable drone should be disabled until blacklist timer reset") {
      val session = new Session(spooky)
      val drone = UAV(Seq("dummy"))
      TestHelper.setLoggerDuring(classOf[Link], classOf[MAVLink], SpookyUtils.getClass) {
        intercept[Throwable] {
          Dispatcher(
            List(drone),
            session
          ).get
        }

        val badLink = Link.registered(drone)
        assert(badLink.statusStr.contains("DRONE@dummy -> unreachable for"))
        //          assert {
        //            val e = badLink.lastFailureOpt.get._1
        //            e.isInstanceOf[AssertionError] //|| e.isInstanceOf[Wrap]
        //          }
      }
    }

    it("Link.connect()/disconnect() should not leave dangling process") {
      val linkRDD: RDD[Link] = getLinkRDD(spooky)
      linkRDD.foreach { link =>
        for (_ <- 1 to 3) {
          link.connect()
          link.disconnect()
        }
      }
      //wait for zombie process to be deregistered
      CommonUtils.retry(5, 2000) {
        sc.runEverywhere() { _ =>
          SpookyEnvFixture.processShouldBeClean(Seq("mavproxy"), Seq("mavproxy"), cleanSweepDrivers = false)

          Link.registered.foreach { v =>
            v._2.disconnect()
          }
        }
      }
    }
  }
}
