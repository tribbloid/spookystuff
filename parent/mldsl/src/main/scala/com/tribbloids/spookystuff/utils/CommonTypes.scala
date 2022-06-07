package com.tribbloids.spookystuff.utils

class CommonTypes {

  type Unary[T] = T => T
  type Binary[T] = (T, T) => T

}

object CommonTypes extends CommonTypes
