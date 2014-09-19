package org.tribbloid.spookystuff.spike

/**
 * Created by peng on 9/16/14.
 */
object TestCaseClass {

  val ff: String => Boolean = _.isEmpty

  case class Case(
                   i: Int,
                   j: Int,
                   f: String => Boolean = ff
                   )(
                   val r: String
                   ){
    var k: String = "k"
  }

  def main(args: Array[String]) {

    val a = Case(2,3)("abc")
    a.k = "modified"
    val b = Case(2,3)("def")
    val c = Case(2,3,{_.isEmpty})("def")

    println(a.toString+"|"+a.hashCode())
    println(a.copy(i = 5)(r = "g").k.toString)
    println(b.toString+"|"+b.hashCode())
    println(c.toString+"|"+c.hashCode())
    println("--------------------------------")
    println(a == b)
    println(a == c)
  }
}
