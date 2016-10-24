package com.tribbloids.spookystuff.session

import com.tribbloids.spookystuff.actions.PyConverter
import com.tribbloids.spookystuff.{PythonException, SpookyEnvFixture}

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
    val proc = new PythonDriver("python", taskOrThread = TaskOrThread())
    try {
      val result = xs.map{
        f(_, proc)
      }
      result
    }
    finally {
      proc.finalize()
    }
  }
}

class PythonDriverSuite extends SpookyEnvFixture {

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
        intercept[PythonException]{
          proc.interpret(s"print($i / 0)")
        }
    }
  }

  test("interpret should throw an exception if interpreter raises a multi-line error") {

    PythonDriverSuite.runIterable(1 to 10) {
      (i, proc) =>
        intercept[PythonException]{
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
        intercept[PythonException]{
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
        val r = proc.execute(s"print($i / 1)")
        assert(r._1.mkString("\n") == i.toString)
        assert(r._2.isEmpty)
    }
  }
}
