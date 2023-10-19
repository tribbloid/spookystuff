/*
 * Copyright 2016 Azavea
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.tribbloids.spookystuff.caching

import com.tribbloids.spookystuff.utils.Caching

//TODO: not efficient should be replaced
trait Memoize[T, R] extends (T => R) with Serializable {

  def f(v: T): R

  val cache = Caching.ConcurrentCache[T, (R, Long)]()

  def get(x: T, condition: ((R, Long)) => Boolean): R = {
    if (cache.contains(x)) {
      val cached = cache(x)
      if (condition(cached))
        return cached._1
    }
    val y = f(x)
    val time = System.currentTimeMillis()
    cache.put(x, y -> time)
    y
  }

  def apply(x: T): R = {
    get(x, _ => true)
  }

  def getIfNotExpire(x: T, expireAfter: Long): R = {
    get(
      x,
      { tuple =>
        val elapsed = System.currentTimeMillis() - tuple._2
        elapsed > expireAfter
      }
    )
  }

  def getLaterThan(x: T, timestamp: Long): R = {
    get(
      x,
      { tuple =>
        tuple._2 > timestamp
      }
    )
  }
}
