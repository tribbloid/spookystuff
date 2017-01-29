package com.tribbloids.spookystuff.mav.telemetry

import com.tribbloids.spookystuff.mav.dsl.{LinkFactories, LinkFactory}
import com.tribbloids.spookystuff.mav.sim.SIMFixture
import com.tribbloids.spookystuff.mav.system.Drone
import com.tribbloids.spookystuff.mav.telemetry.mavlink.MAVLink
import com.tribbloids.spookystuff.mav.{MAVConf, ReinforcementDepletedException}
import com.tribbloids.spookystuff.session.Session
import com.tribbloids.spookystuff.testutils.TestHelper
import com.tribbloids.spookystuff.utils.SpookyUtils
import com.tribbloids.spookystuff.utils.TreeException.MultiCauseWrapper
import com.tribbloids.spookystuff.{PyInterpretationException, SpookyContext, SpookyEnvFixture}
import org.apache.spark.rdd.RDD

import scala.util.Random

abstract class LinkFixture extends SIMFixture {

  import com.tribbloids.spookystuff.utils.SpookyViews._

  lazy val listDrones: String => Seq[Drone] = {
    connStr =>
      Seq(Drone(Seq(connStr)))
  }

  override def setUp(): Unit = {

    super.setUp()
    sc.foreachCore {
      Random.shuffle(Link.existing.values.toList).foreach(_.clean())
    }
    //    Thread.sleep(2000) //Waiting for python drivers to terminate
  }

  //  override def tearDown(): Unit = {
  //    sc.foreachComputer {
  //      ResourceLedger.detectConflict()
  //    }
  //    super.tearDown()
  //  }

  def getSpooky(factory: LinkFactory): (SpookyContext, String) = {

    val conf = this.spooky.conf.clone
    val spooky = this.spooky.copy(spookyConf = conf)
    spooky.submodule[MAVConf].linkFactory = factory
    spooky.rebroadcast()

    val name = spooky.submodule[MAVConf].linkFactory.getClass.getSimpleName
    spooky -> s"linkFactory=$name:"
  }


  protected def getLinkRDD(spooky: SpookyContext) = {
    val listDrones = this.listDrones
    val linkRDD = simURIRDD.map {
      connStr =>
        val endpoint = Drone(Seq(connStr))
        val session = new Session(spooky)
        val link = Link.trySelect(
          listDrones(connStr),
          session
        )
          .get
        TestHelper.assert(link.isNotBlacklisted, "link is blacklisted")
        link.isIdle = false
//        Thread.sleep(5000) //otherwise a task will complete so fast such that another task hasn't start yet.
        link
    }
      .persist()
    val uriRDD = linkRDD.map {
      link =>
        link.drone.uris.head
    }
    val uris = uriRDD.collect()
    assert(uris.distinct.length == this.parallelism, "Duplicated URIs:\n" + uris.mkString("\n"))
    linkRDD
  }

  val tuples = Seq(
    LinkFactories.Direct,
    LinkFactories.ForkToGCS()
  ).map {
    getSpooky
  }
  tuples.foreach {
    case (spooky, testPrefix) =>

      test(s"$testPrefix Link should use different drones") {
        val linkRDD: RDD[Link] = getLinkRDD(spooky)
      }

      test(s"$testPrefix Link to non-existing drone should be disabled until blacklist timer reset") {
        val session = new Session(spooky)
        val drone = Drone(Seq("dummy"))
        TestHelper.setLoggerDuring(classOf[Link], classOf[MAVLink]) {
          intercept[ReinforcementDepletedException]{
            Link.trySelect(
              Seq(drone),
              session
            )
              .get
          }

          val badLink = Link.existing(drone)
          badLink.statusString.shouldBeLike(
            "Link DRONE@dummy is unreachable for ......"
          )
          assert {
            val e = badLink.lastFailureOpt.get._1
            e.isInstanceOf[PyInterpretationException] || e.isInstanceOf[MultiCauseWrapper]
          }
        }
      }

      test(s"$testPrefix Link.connect()/disconnect() should not leave dangling process") {
        val linkRDD: RDD[Link] = getLinkRDD(spooky)
        linkRDD.foreach {
          link =>
            for (i <- 1 to 2) {
              link.connect()
              link.disconnect()
            }
        }
        //wait for zombie process to be deregistered
        SpookyUtils.retry(5, 2000) {
          sc.foreachComputer {
            SpookyEnvFixture.processShouldBeClean(Seq("mavproxy"), Seq("mavproxy"), cleanSweepNotInTask = false)
          }
        }
      }

      test(s"$testPrefix Link created in the same TaskContext should be reused") {

        val listDrones = this.listDrones
        val linkStrs = simURIRDD.map {
          connStr =>
            val endpoints = listDrones(connStr)
            val session = new Session(spooky)
            val link1 = Link.trySelect (
              endpoints,
              session
            )
              .get
            val link2 = Link.trySelect (
              endpoints,
              session
            )
              .get
            Thread.sleep(5000) //otherwise a task will complete so fast such that another task hasn't start yet.
          val result = link1.toString -> link2.toString
            result
        }
          .collect()
        assert(spooky.metrics.linkCreated.value == parallelism)
        //        assert(spooky.metrics.linkDestroyed.value == 0) // not testable, refit always destroy previous link
        linkStrs.foreach {
          tuple =>
            assert(tuple._1 == tuple._2)
        }
      }

      test(s"$testPrefix idle Link with no active TaskContext can be reused in another TaskContext") {


        val linkRDD1: RDD[Link] = getLinkRDD(spooky)

        assert(spooky.metrics.linkCreated.value == parallelism)
        assert(spooky.metrics.linkDestroyed.value == 0)
        assert(Link.existing.size == parallelism)

        linkRDD1.foreach {
          link =>
            link.isIdle = true
        }

        val linkRDD2: RDD[Link] = getLinkRDD(spooky)

        assert(spooky.metrics.linkCreated.value == parallelism)
        assert(spooky.metrics.linkDestroyed.value == 0)
        linkRDD1.map(_.toString).collect().mkString("\n").shouldBe (
          linkRDD2.map(_.toString).collect().mkString("\n"),
          sort = true
        )
      }
  }
}
