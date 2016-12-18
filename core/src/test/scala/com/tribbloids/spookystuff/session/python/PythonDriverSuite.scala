package com.tribbloids.spookystuff.session.python

import com.tribbloids.spookystuff.session.Lifespan
import com.tribbloids.spookystuff.testutils.TestHelper
import com.tribbloids.spookystuff.utils.SpookyUtils
import com.tribbloids.spookystuff.{PyInterpreterException, SpookyEnvFixture}
import org.slf4j.LoggerFactory

import scala.concurrent.Future
import scala.util.Try

/**
  * Created by peng on 01/08/16.
  */
object PythonDriverSuite {

  def onePlusX(xs: Seq[Int]): Unit = {
    runIterable(xs) {
      (i, proc) =>
        val r = proc.sendAndGetResult(s"print($i + 1)")
        assert(r.replace(">>> ", "").trim == (i + 1).toString)
    }
  }

  def runIterable[T, R](xs: Iterable[T])(f: (T, PythonDriver) => R): Iterable[R] = {
    val proc = new PythonDriver("python", lifespan = new Lifespan.Auto(Some("testPython")))
    try {
      val result = xs.map{
        f(_, proc)
      }
      result
    }
    finally {
      proc.tryClean()
    }
  }
}

class PythonDriverSuite extends SpookyEnvFixture {

  import scala.concurrent.duration._
  import scala.concurrent.ExecutionContext.Implicits.global

  test("sendAndGetResult should work in single thread") {
    PythonDriverSuite.onePlusX(1 to 100)
  }

  test("sendAndGetResult should work in multiple threads") {
    val rdd = sc.parallelize(1 to 100)
    assert(rdd.partitions.length > 1)
    rdd.foreachPartition{
      it =>
        val seq = it.toSeq
        PythonDriverSuite.onePlusX(seq)
    }
  }

  test("sendAndGetResult should work if interpretation triggers an error") {
    PythonDriverSuite.runIterable(1 to 10) {
      (i, proc) =>
        val r = proc.sendAndGetResult(s"print($i / 0)")
        assert(r.replace(">>> ", "").trim startsWith "Traceback")
    }
  }

  test("interpret should yield 1 row for a single print") {

    PythonDriverSuite.runIterable(1 to 10) {
      (i, proc) =>
        val result = proc.interpret(s"print($i * $i)").mkString("\n")
        result.shouldBe(
          "" + i*i
        )
    }
  }

  test("interpret should throw an exception if interpreter raises an error") {

    PythonDriverSuite.runIterable(1 to 10) {
      (i, proc) =>
        intercept[PyInterpreterException]{
          proc.interpret(s"print($i / 0)")
        }
    }
  }

  test("interpret should throw an exception if interpreter raises a multi-line error") {

    PythonDriverSuite.runIterable(1 to 10) {
      (i, proc) =>
        intercept[PyInterpreterException]{
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

  test("interpret should throw an exception if interpreter raises a syntax error") {

    PythonDriverSuite.runIterable(1 to 10) {
      (i, proc) =>
        intercept[PyInterpreterException]{
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

  test("call should return None if result variable is undefined") {

    PythonDriverSuite.runIterable(1 to 10) {
      (i, proc) =>
        val r = proc.eval(s"print($i / 1)")
        assert(r._1.mkString("\n") == i.toString)
        assert(r._2.isEmpty)
    }
  }

  test("SpookyUtils.withDeadline can interrupt python execution that blocks indefinitely") {

    PythonDriverSuite.runIterable(1 to 3) {
      (i, proc) =>
        proc.batchImport(Seq("import time"))
        val (_, time) = TestHelper.timer {
          Try {SpookyUtils.withDeadline(5.seconds) {
            proc.interpret(
              s"""
                 |for i in range(10, 1, -1):
                 |  print("sleeping:", i, "second(s) left")
                 |  time.sleep(1)
             """.stripMargin)
          }}
        }
        assert(time <= 6000)
        println("============== SUCCESS!!!!!!!!!!! ==============")
    }
  }

  test("clean() won't be blocked indefinitely by ongoing python execution") {

    PythonDriverSuite.runIterable(1 to 3) {
      (i, proc) =>
        proc.batchImport(Seq("import time"))
        val f = Future {
          proc.interpret(
            s"""
               |for i in range(40, 1, -1):
               |  print("sleeping:", i, "second(s) left")
               |  time.sleep(1)
             """.stripMargin)
        }

        LoggerFactory.getLogger(this.getClass).info("========= START CLEANING =========")
        SpookyUtils.withDeadline(20.seconds, Some(1.second)) {
          proc.clean()
        }
    }
  }
}
