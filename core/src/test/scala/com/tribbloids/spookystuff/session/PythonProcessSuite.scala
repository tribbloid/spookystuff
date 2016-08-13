package com.tribbloids.spookystuff.session

import com.tribbloids.spookystuff.SpookyEnvSuite
import com.tribbloids.spookystuff.utils.PythonProcess

/**
  * Created by peng on 01/08/16.
  */
object PythonProcessSuite {
  def run1PlusX(xs: Seq[Int]): Unit = {

    val proc = new PythonProcess("python")
    try {
      proc.open()
      xs.foreach{
        x =>
          val r = proc.sendAndGetResult(s"print($x + 1)")
          assert (r.replace(">>> ","").trim == (x + 1).toString)
      }
    }
    finally {
      proc.close()
    }
  }
}

class PythonProcessSuite extends SpookyEnvSuite {

  test("should work in single thread") {
    PythonProcessSuite.run1PlusX(1 to 100)
  }

  test("should work in multiple threads") {
    val rdd = sc.parallelize(1 to 1000)
    rdd.foreachPartition{
      it =>
        val seq = it.toSeq
        PythonProcessSuite.run1PlusX(seq)
    }
  }
}
