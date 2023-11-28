package com.tribbloids.spookystuff.utils

import java.security.PrivilegedAction

import scala.collection.mutable

abstract class CommonViews {

  @transient lazy val specialChars: String = "(?=[]\\[+$&|!(){}^\"~*?:\\\\-])"

  implicit class StringView(str: String) {

    def :/(other: String): String = CommonUtils./:/(str, other)
    def \\(other: String): String = CommonUtils.\\\(str, other)
  }

  implicit class Function2PrivilegedAction[T](f: => T) extends PrivilegedAction[T] {
    override def run(): T = {
      f
    }
  }

  implicit class MutableMapView[K, V](self: mutable.Map[K, V]) {

    def getOrElseUpdateSynchronously(key: K)(value: => V): V = {

      self.getOrElse(
        key, {
          self.synchronized {
            self.getOrElseUpdate(key, value)
          }
        }
      )
    }
  }
}

object CommonViews extends CommonViews
