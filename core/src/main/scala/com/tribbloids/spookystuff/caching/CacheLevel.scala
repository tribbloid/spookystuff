package com.tribbloids.spookystuff.caching

/**
  * Created by peng on 29/01/17.
  */
object CacheLevel {

  trait Value extends Serializable

  object None extends Value

  trait InMemory extends Value
  object InMemory extends InMemory

  trait DFS extends Value
  object DFS extends DFS

  object All extends InMemory with DFS
}
