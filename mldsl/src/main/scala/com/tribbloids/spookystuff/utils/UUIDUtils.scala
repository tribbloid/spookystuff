package com.tribbloids.spookystuff.utils

import java.util.UUID

object UUIDUtils {

  def xor(v1: UUID, v2: UUID): UUID = {

    val higher = v1.getMostSignificantBits ^ v2.getMostSignificantBits
    val lower = v1.getLeastSignificantBits ^ v2.getLeastSignificantBits
    new UUID(higher, lower)
  }
}
