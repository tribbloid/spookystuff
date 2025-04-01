package org.apache.spark.benchmark

import org.apache.spark.benchmark.__inline.Benchmark

import java.io.OutputStream
import scala.concurrent.duration.{DurationInt, FiniteDuration}

case class BenchmarkHelper(
    name: String,
    valuesPerIteration: Long = 1,
    minNumIters: Int = 2,
    warmupTime: FiniteDuration = 2.seconds,
    minTime: FiniteDuration = 2.seconds,
    outputPerIteration: Boolean = false,
    output: Option[OutputStream] = None
) {

  val self: Benchmark =
    new Benchmark(name, valuesPerIteration, minNumIters, warmupTime, minTime, outputPerIteration, output)
}
