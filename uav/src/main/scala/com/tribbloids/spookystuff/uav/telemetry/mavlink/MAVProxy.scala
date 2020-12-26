package com.tribbloids.spookystuff.uav.telemetry.mavlink

import java.util.concurrent.{ExecutorService, Executors, TimeUnit}

import com.tribbloids.spookystuff.python.ref.{BindedRef, CaseInstanceRef}
import com.tribbloids.spookystuff.session.{ConflictDetection, PythonDriver}
import com.tribbloids.spookystuff.uav.{UAVConf, UAVException}
import com.tribbloids.spookystuff.utils.FutureInterruptable
import com.tribbloids.spookystuff.utils.lifespan.Cleanable

import scala.concurrent.{Await, ExecutionContext, ExecutionContextExecutor, TimeoutException}
import scala.util.{Failure, Success, Try}

object MAVProxy {

  val threadPool: ExecutorService = Executors.newCachedThreadPool()
  val executionContext: ExecutionContextExecutor = ExecutionContext.fromExecutor(threadPool)
}

/**
  * MAVProxy: https://github.com/ArduPilot/MAVProxy
  * outlives any python driver
  * not to be confused with dsl.WebProxy
  * CAUTION: each MAVProxy instance contains 2 python processes, keep that in mind when debugging
  */
//TODO: MAVProxy supports multiple master for multiple telemetry backup
case class MAVProxy(
    master: String,
    outs: Seq[String], //first member is always used by DK.
    baudRate: Int,
    ssid: Int = UAVConf.PROXY_SSID,
    name: String
)(
    override val driverTemplate: PythonDriver
) extends CaseInstanceRef
    with BindedRef
    with ConflictDetection {

  assert(!outs.contains(master))
  override lazy val _resourceIDs = Map(
    "master" -> Set(master),
    "firstOut" -> outs.headOption.toSet //need at least 1 out for executor
  )

  @volatile var _started: FutureInterruptable[String] = _
  def startedOpt: Option[FutureInterruptable[String]] = {
    Option(_started).flatMap { v =>
      if (v.isCompleted) {
        _started = null
        None
      } else Some(v)
    }
  }

  // should always restart?
  def start(): Unit = this.synchronized {
    implicit val ctx = MAVProxy.executionContext

    startedOpt.getOrElse {
      val attempt: FutureInterruptable[String] = FutureInterruptable {
        val resultOpt = this.PY.startAndBlock().$STR
        resultOpt.getOrElse("NONE")
      }

      def errorInfo = s"MAVProxy is terminated! perhaps due to unreachable endpoint ${this.master}"

      //      attempt.onComplete {
      //        case Success(v) =>
      //          LoggerFactory.getLogger(this.getClass).error("IMPOSSIBLE! " + v)
      //        case Failure(e) =>
      //          LoggerFactory.getLogger(this.getClass).error(errorInfo, e)
      //      }

      Try(Await.result(attempt, 5 -> TimeUnit.SECONDS)) match {
        case Failure(e: TimeoutException) => //normal
        case Failure(e: Throwable) =>
          throw new UAVException(errorInfo, e)
        case Success(v) => sys.error("IMPOSSIBLE!")
      }

      this._started = attempt
    }
  }

  override def stopDriver(): Unit = this.synchronized {
    Option(_driver).foreach { v =>
      v.closeOrInterrupt()
      v.clean(true)
    }
    _driver = null
    for (future <- startedOpt) {
      future.interrupt()
    }
    _started = null
  }

  override def chainClean: Seq[Cleanable] = Nil //interpreter is blocked and cannot run any delete code
}

//object MAVProxy {
//
//  val _OPTIONS = Seq(
//    "--state-basedir=temp",
//    "--daemon",
//    "--default-modules=\"link\""
//  )
//
//  //only use reflection to find UNIXProcess.pid.
//  @throws[NoSuchFieldException]
//  @throws[IllegalAccessException]
//  private def findPid(process: Process) = {
//    var pid = -1L
//    if (process.getClass.getName == "java.lang.UNIXProcess") {
//      val f = process.getClass.getDeclaredField("pid")
//      f.setAccessible(true)
//      pid = f.getLong(process)
//      f.setAccessible(false)
//    }
//    pid
//  }
//
//  private def doClean(p: Process, pid: Long) = {
//    Try {
//      p.destroy()
//    }
//      .recoverWith {
//        case e: Exception =>
//          Try {
//            p.destroyForcibly()
//          }
//      }
//      .recover {
//        case e: Exception =>
//          if (pid > -1) {
//            Runtime.getRuntime.exec("kill -SIGINT " + pid)
//          }
//          else {
//            p.destroyForcibly()
//          }
//      }
//  }
//}

//case class MAVProxy(
//                     master: String,
//                     outs: Seq[String], //first member is always used by DK.
//                     baudRate: Int,
//                     ssid: Int = UAVConf.PROXY_SSID,
//                     name: String
//                   )
//  extends LocalCleanable
//    with ConflictDetection {
//
//  assert(!outs.contains(master))
//
//  override lazy val _resourceIDs = Map(
//    "master" -> Set(master),
//    "firstOut" -> outs.headOption.toSet //need at least 1 out for executor
//  )
//
//  override def _lifespan = new Lifespan.JVM(
//    nameOpt = Some(this.getClass.getSimpleName)
//  )
//
//  @transient var _process_pid: (Process, Long) = _
//
//  def process_pidOpt: Option[(Process, Long)] = Option(_process_pid).flatMap {
//    v =>
//      if (!v._1.isAlive) {
//        MAVProxy.doClean(v._1, v._2)
//        _process_pid = null
//        None
//      }
//      else Some(v)
//  }
//
//  val commandStrs = {
//    val MAVPROXY = sys.env.getOrElse("MAVPROXY_CMD", "mavproxy.py")
//
//    val strs = new ArrayBuffer[String]()
//    strs append MAVPROXY
//    strs append s"--master=$master"
//    for (out <- outs) {
//      strs append s"--out=$out"
//    }
//    strs append s"--baudrate=$baudRate"
//    strs append s"--source-system=$ssid"
//    strs appendAll MAVProxy._OPTIONS
//
//    //    LoggerFactory.getLogger(classOf[Proxy]).info(strs.mkString(" "))
//    strs
//  }
//
//  def open(): Unit = {
//    process_pidOpt.getOrElse {
//      CommonUtils.retry(2, 1000) {
//        _doOpen()
//      }
//    }
//    Thread.sleep(2000)
//    if (process_pidOpt.isEmpty)
//      throw new UAVException(s"MAVProxy is terminated! perhaps due to non-existing master URI ${this.master}")
//  }
//
//  def _doOpen(): Unit = {
//    val builder = new ProcessBuilder(commandStrs: _*)
//    builder.redirectErrorStream(true)
//    val process = builder.start
//
//    val pid = try
//      MAVProxy.findPid(process)
//    catch {
//      case e: Exception =>
//        -1
//    }
//    _process_pid = process -> pid
//  }
//
//  def closeProcess(): Unit = {
//
//    process_pidOpt.foreach {
//      v =>
//        MAVProxy.doClean(v._1, v._2)
//        _process_pid = null
//    }
//
//    assert(process_pidOpt.isEmpty)
//  }
//
//  override protected def cleanImpl(): Unit = {
//    closeProcess()
//  }
//}
