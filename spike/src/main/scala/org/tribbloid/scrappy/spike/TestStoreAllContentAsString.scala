package org.tribbloid.scrappy.spike

import java.net.{URLConnection, URL}

import org.apache.commons.io.IOUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.deploy.SparkHadoopUtil
import org.openqa.selenium.phantomjs.PhantomJSDriver
import org.tribbloid.spookystuff.Conf

/**
 * Created by peng on 17/06/14.
 */
object TestStoreAllContentAsString {

  def main(args: Array[String]) {

        var uri = "http://www.google.com"
//    val uri = "http://col.stb01.s-msn.com/i/74/A177116AA6132728F299DCF588F794.gif"
//    val uri = "http://www.cs.toronto.edu/~ranzato/publications/DistBeliefNIPS2012_withAppendix.pdf"

//    val driver = new PhantomJSDriver(Conf.phantomJSCaps)
//    driver.get(uri)
//    val content = driver.getPageSource.getBytes("UTF8")

    var uc: URLConnection = null
    uc = new URL(uri).openConnection()

    uc.connect()
    val is = uc.getInputStream()

    val content = IOUtils.toByteArray(is)
    is.close()

    println(uc.getContentType)
    println(uc.getContentEncoding)

    val fullPath = new Path("file:///home/peng/spookystuff/test")

    //    val bufferSize = sc.getConf.getInt("spark.buffer.size", 65536)

    val hConf: Configuration = SparkHadoopUtil.get.newConfiguration()
    val fs = fullPath.getFileSystem(hConf)

    val fileOutputStream = fs.create(fullPath, true)

    IOUtils.write(content,fileOutputStream)

    fileOutputStream.close()

    //    fileOutputStream.write(bytes)
    //
    //    fileOutputStream.flush()
    //
    //    fileOutputStream.close()

    //    val writer = new BufferedWriter(new OutputStreamWriter(fileOutputStream,"UTF-8")) //why using two buffers
    //    val writer = new OutputStreamWriter(fileOutputStream)
    //    writer.write(chars)
  }
}
