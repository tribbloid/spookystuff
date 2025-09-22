package com.tribbloids.spookystuff.commons

import java.security.PrivilegedAction

abstract class CommonViews {

  @transient lazy val specialChars: String = "(?=[]\\[+$&|!(){}^\"~*?:\\\\-])"

  implicit class Function2PrivilegedAction[T](f: => T) extends PrivilegedAction[T] {
    override def run(): T = {
      f
    }
  }
}

object CommonViews extends CommonViews
