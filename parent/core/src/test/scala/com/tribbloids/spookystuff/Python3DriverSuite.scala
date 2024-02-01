package com.tribbloids.spookystuff

import com.tribbloids.spookystuff.python.PyConverter
import Python3DriverSuite.Runner
import com.tribbloids.spookystuff.agent.PythonDriver
import com.tribbloids.spookystuff.testutils.SpookyBaseSpec
import com.tribbloids.spookystuff.utils.lifespan.Cleanable.Lifespan
import com.tribbloids.spookystuff.utils.CommonUtils
import org.slf4j.LoggerFactory

import scala.concurrent.Future
import scala.util.Try

object Python3DriverSuite {

  case class Runner(pythonExec: String) {
    val onePlusX: Seq[Int] => Unit = { xs =>
      runIterable(xs) { (i, proc) =>
        val r = proc.sendAndGetResult(s"print($i + 1)")
        assert(r.replace(">>> ", "").trim == (i + 1).toString)
      }
    }

    def runIterable[T, R](xs: Iterable[T])(f: (T, PythonDriver) => R): Iterable[R] = {
      val pythonExec = this.pythonExec
      val proc = new PythonDriver(
        pythonExec,
        _lifespan = Lifespan
          .TaskOrJVM(
            nameOpt = Some("testPython")
          )
          .forShipping
      )
      try {
        val result = xs.map {
          f(_, proc)
        }
        result
      } finally {
        proc.tryClean()
      }
    }
  }

  object Runner3 extends Runner("python3")
}

/**
  * Created by peng on 01/08/16.
  */
class Python3DriverSuite extends SpookyBaseSpec {

  import scala.concurrent.ExecutionContext.Implicits.global
  import scala.concurrent.duration._

  override def afterAll(): Unit = {

    Thread.sleep(3000) // wait for zombie process to die
    super.afterAll()
  }

  lazy val runner: Runner = Python3DriverSuite.Runner3
  import runner._

  it("sendAndGetResult should work in single thread") {
    onePlusX(1 to 100)
  }

  it("sendAndGetResult should work in multiple threads") {

    val runner: Runner = this.runner
    import runner._

    val rdd = sc.parallelize(1 to 100)
    assert(rdd.partitions.length > 1)
    rdd.foreachPartition { it =>
      val seq = it.toSeq
      onePlusX(seq)
    }
  }

  it("sendAndGetResult should work if interpretation triggers an error") {
    runIterable(1 to 10) { (i, proc) =>
      val r = proc.sendAndGetResult(s"print($i / 0)")
      assert(r.replace(">>> ", "").trim startsWith "Traceback")
    }
  }

  it("interpret should yield 1 row for a single print") {

    runIterable(1 to 10) { (i, proc) =>
      val result = proc.interpret(s"print($i * $i)").mkString("\n")
      result.shouldBe(
        "" + i * i
      )
    }
  }

  it("interpret should throw an exception if interpreter raises an error") {

    runIterable(1 to 10) { (i, proc) =>
      intercept[PyInterpretationException] {
        proc.interpret(s"print($i / 0)")
      }
    }
  }

  it("interpret should throw an exception if interpreter raises a multi-line error") {

    runIterable(1 to 10) { (_, proc) =>
      intercept[PyInterpretationException] {
        proc.interpret(
          s"""
               |raise Exception(
               |${PyConverter.QQQ}
               |abc
               |def
               |ghi
               |jkl
               |${PyConverter.QQQ}
               |)
            """.stripMargin
        )
      }
    }
  }

  it("interpret should throw an exception if interpreter raises a syntax error") {
    // TODO: this syntax error is really weird

    runIterable(1 to 10) { (_, proc) =>
      intercept[PyInterpretationException] {
        proc.interpret(
          s"""
               |dummyPyAction4196844262929992980=List(py, s, p, o, o, k, y, s, t, u, f, f, ., m, a, v, ., a, c, t, i, o, n, s, ., D, u, m, m, y, P, y, A, c, t, i, o, n)(**(json.loads(
               |${PyConverter.QQQ}
               |{
               |  "className" :"com.tribbloids.spookystuff.mav.actions.DummyPyAction",
               |  "params" : {
               |    "a" : {
               |      "value" : 1,
               |      "dataType" : { }
               |    }
               |  }
               |}
               |
              |${PyConverter.QQQ}
               |)))
            """.stripMargin
        )
      }
    }
  }

  it("call should return None if result variable is undefined") {

    runIterable(1 to 10) { (i: Int, proc) =>
      val r = proc.eval(s"print($i / 1)")
      assert(
        Seq(i.toString, i.toString + ".0").contains(r._1.mkString("\n"))
      )
      assert(r._2.isEmpty)
    }
  }

  it("can use the correct python version") {
    runIterable(1 to 1) { (_, proc) =>
      val r = proc.eval(
        """
            |import sys
            |
            |print(sys.version_info[0])
          """.stripMargin
      )

      assert(s"python${r._1.mkString("\n")}" == this.runner.pythonExec)
    }
  }

  it("CommonUtils.withDeadline can interrupt python execution that blocks indefinitely") {

    runIterable(1 to 3) { (_, proc) =>
      proc.batchImport(Seq("import time"))
      val (_, time) = CommonUtils.timed {
        Try {
          CommonUtils.withTimeout(5.seconds) {
            proc.interpret(s"""
                 |for i in range(10, 1, -1):
                 |  print("sleeping:", i, "second(s) left")
                 |  time.sleep(1)
             """.stripMargin)
          }
        }
      }
      assert(time <= 6000)
      println("============== SUCCESS!!!!!!!!!!! ==============")
    }
  }

  it("clean() won't be blocked indefinitely by ongoing python execution") {

    runIterable(1 to 3) { (_, proc) =>
      proc.batchImport(Seq("import time"))
      Future {
        proc.interpret(s"""
               |for i in range(40, 1, -1):
               |  print("sleeping:", i, "second(s) left")
               |  time.sleep(1)
             """.stripMargin)
      }

      LoggerFactory.getLogger(this.getClass).info("========= START CLEANING =========")
      CommonUtils.withTimeout(
        (CommonConst.driverClosingTimeout * CommonConst.driverClosingRetries) + 5.seconds,
        1.second
      ) {
        proc.clean()
      }
    }
  }
}
