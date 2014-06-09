package org.tribbloid.scrappy.spike

/**
 * Created by peng on 08/06/14.
 */
object TestSerialization {

  def main(args: Array[String]) {
    val a = Seq[Int](1, 2, 3, 5)
    val b = Seq[String]("rc",null,"bc")
    val ab = (a, b, null)
    println(ab.toString())
  }
}
