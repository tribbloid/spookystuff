package com.tribbloids.spookystuff.session

import com.tribbloids.spookystuff.actions.PyAction
import com.tribbloids.spookystuff.{PythonException, SpookyEnvFixture}

/**
  * Created by peng on 01/08/16.
  */
object PythonDriverSuite {

  def send1PlusX(xs: Seq[Int]): Unit = {
    runIterable(xs) {
      (i, proc) =>
        val r = proc.sendAndGetResult(s"print($i + 1)")
        assert(r.replace(">>> ", "").trim == (i + 1).toString)
    }
  }

  def runIterable[T, R](xs: Iterable[T])(f: (T, PythonDriver) => R): Iterable[R] = {
    val proc = new PythonDriver("python")
    proc.open()
    try {
      val result = xs.map{
        f(_, proc)
      }
      result
    }
    finally {
      proc.close()
    }
  }
}

class PythonDriverSuite extends SpookyEnvFixture {

  test("sendAndGetResult should work in single thread") {
    PythonDriverSuite.send1PlusX(1 to 100)
  }

  test("sendAndGetResult should work in multiple threads") {
    val rdd = sc.parallelize(1 to 100)
    assert(rdd.getNumPartitions > 1)
    rdd.foreachPartition{
      it =>
        val seq = it.toSeq
        PythonDriverSuite.send1PlusX(seq)
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
          val result = proc.interpret(
            s"""
               |raise Exception(
               |${PyAction.QQQ}
               |abc
               |def
               |ghi
               |jkl
               |${PyAction.QQQ}
               |)
            """.stripMargin
          )
        }
    }
  }
}
