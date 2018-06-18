package com.tribbloids.spookystuff.example

import java.util.Properties

import com.tribbloids.spookystuff.SpookyContext
import com.tribbloids.spookystuff.dsl.Samplers
import com.tribbloids.spookystuff.utils.CommonConst
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
 * Created by peng on 22/06/14.
 * allowing execution as a main object and tested as a test class
 * keep each test as small as possible, by using downsampling & very few iterations
 */
trait SpookyEnv {

  val appName = this.getClass.getSimpleName.replace("$","")

  val spark = {

    val conf: SparkConf = new SparkConf().setAppName(appName)

    val master = conf.getOption("spark.master")
      .orElse(Option(System.getenv("MASTER")))
      .getOrElse(s"local[${Runtime.getRuntime.availableProcessors()},10]")

    conf.setMaster(master)

    SparkSession.builder().config(conf).appName(appName).getOrCreate()
  }
  val sc = spark.sparkContext
  val sql = spark.sqlContext

  def sampler = Samplers.FirstN(3)
  def maxExploreDepth = 2
  var maxInputSize = 3

  def getSpooky(args: Array[String]): SpookyContext = {

    val spooky = new SpookyContext(sql)

    val dirs = spooky.dirConf

    if (dirs.root == null){
      dirs.root = s"file://${CommonConst.USER_DIR}/temp/spooky-local/$appName/"
    }

    if (dirs.cache == null){
      dirs.cache = s"file://${CommonConst.USER_DIR}/temp/spooky-local/cache/"
    }

    val p = new Properties()
    p.load(this.getClass.getResourceAsStream("/conf.properties"))

    val preview = args.headOption.orElse(
      Option(System.getProperty("spooky.preview.mode"))
    )
      .getOrElse(p.getProperty("spooky.preview.mode"))
    if (preview == "preview") {
      spooky.spookyConf.defaultJoinSampler = sampler
      spooky.spookyConf.defaultExploreRange = 0 to maxExploreDepth
    }
    else {
      this.maxInputSize = Int.MaxValue
    }

    spooky.spookyConf.shareMetrics = true

    spooky
  }
}