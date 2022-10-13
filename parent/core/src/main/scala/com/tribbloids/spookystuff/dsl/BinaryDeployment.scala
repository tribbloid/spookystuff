package com.tribbloids.spookystuff.dsl

import com.tribbloids.spookystuff.utils.io.{LocalResolver, URLConnectionResolver, WriteMode}
import com.tribbloids.spookystuff.utils.{CommonUtils, Retry, SpookyUtils, TreeThrowable}
import org.apache.commons.io.IOUtils
import org.apache.spark.{SparkContext, SparkFiles}
import org.slf4j.LoggerFactory

import java.io.File
import java.nio.file.attribute.PosixFilePermission
import scala.util.Try

trait BinaryDeployment extends Serializable {

  def localPath: String
  def remoteURL: String

  final lazy val localFileName = CommonUtils.uri2fileName(localPath)

  import BinaryDeployment._

  val MIN_SIZE_K: Double = 1024.0

  def verifyLocalPath: String

  case class OnDriver(sparkContext: SparkContext) {

    // can only be called on worker
    lazy val deployOnce: Unit = {

      val downloaded = Retry.FixedInterval(3) {

        TreeThrowable
          .|||^(
            Seq(
              { () =>
                verifyLocalPath
              },
              { () =>
                // download from remoteURI to localURI
                val remoteResolver = URLConnectionResolver(10000)
                val localResolver = LocalExeResolver

                remoteResolver.input(remoteURL) { i =>
                  localResolver.output(localPath, WriteMode.Overwrite) { o =>
                    IOUtils.copy(i.stream, o.stream)
                  }
                }

                verifyLocalPath
              }
            ),
            agg = { seq =>
              new UnsupportedOperationException(
                s"${this.getClass.getSimpleName} cannot find resource for deployment, " +
                  s"please provide Internet Connection or deploy manually",
                TreeThrowable.combine(seq)
              )
            }
          )
          .get
      }

      val localFileName = CommonUtils.uri2fileName(downloaded)

      val existingFiles = sparkContext.listFiles()
      val noNeedToAdd = existingFiles.contains(localFileName)

      if (noNeedToAdd) {

        LoggerFactory.getLogger(this.getClass).info(s"Source `$localFileName` is already already deployed")
      } else {

        LoggerFactory.getLogger(this.getClass).info(s"Deploying `$localFileName` from `$downloaded``")
        sparkContext.addFile(downloaded)
      }
    }
  }

  /**
    * do nothing if local already exists. otherwise download from driver never download from worker(s)! assuming no
    * connection and cached local file
    */
  lazy val verifiedLocalPath: String = {

    val result: Option[String] = TreeThrowable.|||^(
      Seq(
        // already exists
        { () =>
          verifyLocalPath
        },
        // copy from Spark local file
        { () =>
          copySparkFile2Local(localFileName, localPath)
          verifyLocalPath
        }
      ),
      agg = { seq =>
        new IllegalStateException(
          s"${this.getClass.getSimpleName} cannot find deployed binary",
          TreeThrowable.combine(seq)
        )
      }
    )
    result.get
  }

}

object BinaryDeployment {

  object LocalExeResolver extends LocalResolver(extraPermissions = Set(PosixFilePermission.OWNER_EXECUTE))

  def copySparkFile2Local(sparkFile: String, dstStr: String): Option[Any] = {

    val srcStr = SparkFiles.get(sparkFile)
    val srcFile = new File(srcStr)
    val dstFile = new File(dstStr)
    SpookyUtils.ifFileNotExist(dstStr) {
      SpookyUtils.treeCopy(srcFile.toPath, dstFile.toPath)
    }
  }
}
