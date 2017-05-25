package com.tribbloids.spookystuff.example

import java.util.Properties

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import com.tribbloids.spookystuff.SpookyContext
import com.tribbloids.spookystuff.dsl.Samplers

/**
 * Created by peng on 22/06/14.
 * allowing execution as a main object and tested as a test class
 * keep each test as small as possible, by using downsampling & very few iterations
 */
trait SpookyEnv {

  val appName = this.getClass.getSimpleName.replace("$","")

  val sc: SparkContext = {
    val conf: SparkConf = new SparkConf().setAppName(appName)

    val master = conf.getOption("spark.master")
    .orElse(Option(System.getenv("MASTER")))
    .getOrElse(s"local[${Runtime.getRuntime.availableProcessors()},10]")

    conf.setMaster(master)
    new SparkContext(conf)
  }

  val sql: SQLContext = {
    new SQLContext(sc)
  }

  def sampler = Samplers.FirstN(3)
  def maxExploreDepth = 2
  var maxInputSize = 3

  def getSpooky(args: Array[String]): SpookyContext = {

    val spooky = new SpookyContext(sql)

    val dirs = spooky.conf.dirConf

    if (dirs.root == null){
      dirs.root = s"file://${System.getProperty("user.dir")}/temp/spooky-local/$appName/"
    }

    if (dirs.cache == null){
      dirs.cache = s"file://${System.getProperty("user.dir")}/temp/spooky-local/cache/"
    }

    val p = new Properties()
    p.load(this.getClass.getResourceAsStream("/conf.properties"))

    val preview = args.headOption.orElse(
      Option(System.getProperty("spooky.preview.mode"))
    )
      .getOrElse(p.getProperty("spooky.preview.mode"))
    if (preview == "preview") {
      spooky.conf.defaultJoinSampler = sampler
      spooky.conf.defaultExploreRange = 0 to maxExploreDepth
    }
    else {
      this.maxInputSize = Int.MaxValue
    }

    spooky.conf.shareMetrics = true

    spooky
  }
}