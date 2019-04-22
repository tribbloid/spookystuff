package com.tribbloids.spookystuff.utils

import java.security.PrivilegedAction

import scala.collection.GenTraversableOnce
import scala.reflect.ClassTag

abstract class CommonViews {

  @transient lazy val specialChars = "(?=[]\\[+$&|!(){}^\"~*?:\\\\-])"

  implicit class StringView(str: String) {

    def :/(other: String): String = CommonUtils./:/(str, other)
    def \\(other: String): String = CommonUtils.\\\(str, other)

  }

  implicit class Function2PrivilegedAction[T](f: => T) extends PrivilegedAction[T] {
    override def run(): T = {
      f
    }
  }

  implicit class GenTraversableOnceView[A](self: GenTraversableOnce[A])(implicit ctg: ClassTag[A]) {

    def longSize: Long = {
      var result = 0L
      for (x <- self) result += 1
      result
    }
  }
}

object CommonViews extends CommonViews
