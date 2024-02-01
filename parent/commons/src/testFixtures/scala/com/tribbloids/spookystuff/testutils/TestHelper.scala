package com.tribbloids.spookystuff.testutils

import ai.acyclic.prover.commons.spark
import ai.acyclic.prover.commons.spark.Envs
import ai.acyclic.prover.commons.spark.TestHelper.{AWSAccessKeyId, AWSSecretKey, S3Path, TestSC}
import com.tribbloids.spookystuff.utils.classpath.ClasspathResolver
import com.tribbloids.spookystuff.utils.lifespan.Cleanable.Lifespan
import com.tribbloids.spookystuff.utils.lifespan.LocalCleanable
import com.tribbloids.spookystuff.utils.CommonUtils
import org.apache.hadoop.fs.FileUtil
import org.slf4j.LoggerFactory

import java.io.File
import scala.language.implicitConversions

object TestHelper extends LocalCleanable {

  implicit def asProverCommonsSparkHelper(v: this.type): spark.TestHelper.type = {
    ai.acyclic.prover.commons.spark.TestHelper
  }

  {
    val (defaultReport: String, shortReport: String) = ClasspathResolver.debug { o =>
      o.default.Files.debugConfs()

      o.default.completeReport -> o.fileNameOnly.completeReport
    }

    if (S3Path.isDefined) println("Test on AWS S3 with credentials provided by rootkey.csv")

    Seq("s3,s3n,s3a").foreach { n =>
      AWSAccessKeyId.foreach {
        System.setProperty(s"spark.hadoop.fs.$n.awsAccessKeyId", _) // TODO: useless here? set in conf directly?
      }
      AWSSecretKey.foreach {
        System.setProperty(s"spark.hadoop.fs.$n.awsSecretAccessKey", _)
      }
    }

    cleanBeforeAndAfterJVM()
  }

  override def _lifespan: Lifespan = Lifespan.HadoopShutdown.BeforeSpark()

  override def cleanImpl(): Unit = {

    println("=============== Stopping Test Spark Context ==============")
    // Suppress the following log error when shutting down in local-cluster mode:
    // Remote RPC client disassociated. Likely due to containers exceeding thresholds, or network issues.
    // Check driver logs for WARN messages.
    // java.lang.IllegalStateException: Shutdown hooks cannot be modified during shutdown
    val logger = org.apache.log4j.Logger.getRootLogger
    logger.setLevel(org.apache.log4j.Level.toLevel("OFF"))

    TestSC.stop()

    //      println("=============== Test Spark Context has stopped ==============")
    cleanBeforeAndAfterJVM()
  }

  def cleanBeforeAndAfterJVM(): Unit = {
    cleanTempDirs(
      Seq(
        Envs.WAREHOUSE_PATH,
        Envs.METASTORE_PATH,
        Envs.USER_TEMP_DIR
      )
    )
  }

  // TODO: clean up S3 as well
  // also, this should become a class that inherit cleanable
  def cleanTempDirs(paths: Seq[String] = Seq(Envs.USER_TEMP_DIR)): Unit = {
    paths.filter(_ != null).foreach { path =>
      try {
        val file = new File(path)
        CommonUtils.retry(3) {
          val absoluteFile = file.getAbsoluteFile
          if (absoluteFile != null && absoluteFile.exists())
            FileUtil.fullyDelete(absoluteFile, true)
        }
      } catch {
        case e: Throwable =>
          LoggerFactory.getLogger(this.getClass).warn("cannot clean tempDirs", e)
      }
    }
  }
}
