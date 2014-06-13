package org.tribbloid.scrappy.spike

/**
 * Created by peng on 12/06/14.
 */
object TestRegex {

  def main(args: Array[String]) {
    val regex ="[^#{}]+"

    var str = "abc{"
    println(str.matches(regex))

    str = "abc"
    println(str.matches(regex))

    str = "#{abc}"
    println(str.matches(regex))

    str = "http://sanjay"
  }
}
