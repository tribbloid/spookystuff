package org.apache.spark

import org.apache.spark.util.Benchmark

case class BenchmarkHelper(
    name: String,
    minNumIters: Int = 2
) {

  val self = new Benchmark(name, 1L, minNumIters = minNumIters)
}
