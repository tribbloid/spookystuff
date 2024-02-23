package com.tribbloids.spookystuff.io

import com.tribbloids.spookystuff.testutils.{BaseSpec, TestHelper}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.ftp.FTPFileSystem
import org.apache.hadoop.fs.{FileSystem, Path}
import org.scalatest.Ignore

@Ignore
class HDFSResolverSpike extends BaseSpec {

  it("HDFSResolver can read from FTP server") {

    val conf = new Configuration(TestHelper.TestSC.hadoopConfiguration)
//    conf.set(FTPFileSystem.FS_FTP_HOST + "mirror.csclub.uwaterloo.ca", "ftp://mirror.csclub.uwaterloo.ca")
    conf.set(FTPFileSystem.FS_FTP_USER_PREFIX + "mirror.csclub.uwaterloo.ca", "anonymous")
//    conf.set(FTPFileSystem.FS_FTP_PASSWORD_PREFIX + "mirror.csclub.uwaterloo.ca", "")

    val resolvers: Seq[URIResolver] = Seq(
      HDFSResolver(() => conf),
      URLConnectionResolver(5000)
    )

    val mds = resolvers.map { resolver =>
      {
//          val path = "ftp://mirror.csclub.uwaterloo.ca/apache/"
        val path = "ftp://mirror.csclub.uwaterloo.ca/apache/spark/spark-1.6.3/spark-1.6.3-bin-hadoop1-scala2.11.tgz"
        val md = resolver.input(path) { in =>
          in.metadata.all
        }
        md
      }
    }

    mds.foreach(println)
  }

  it("low level test case") {

    val conf = new Configuration()
    conf.set(FTPFileSystem.FS_FTP_USER_PREFIX + "mirror.csclub.uwaterloo.ca", "anonymous")

    val url = "ftp://mirror.csclub.uwaterloo.ca/apache/"
    val path = new Path(url)
    val fs: FileSystem = path.getFileSystem(conf)

    val list = fs.listFiles(path, false)
    while (list.hasNext) {
      println(list.next())
    }
  }
}
