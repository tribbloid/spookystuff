package com.tribbloids.spookystuff.integration

import com.tribbloids.spookystuff.SpookyEnv
import com.tribbloids.spookystuff.rdd.FetchedDataset
import com.tribbloids.spookystuff.testutils.TestHelper

/**
  * move the entire http://webscraper.io/test-sites/ into a local dir for integration tests
  * may use wayback machine:
  * https://web.archive.org/web/20170707111752/http://webscraper.io:80/test-sites
  */
object SnapshotRunner extends SpookyEnv {

  val SPLITTER = "/http://webscraper.io:80"

  import scala.concurrent.duration._
  val cooldown = Some(5.seconds)

  implicit class FDSView(fd: FetchedDataset) {

    import com.tribbloids.spookystuff.dsl.DSL._
    import com.tribbloids.spookystuff.utils.CommonViews.StringView

    def save() = {
      val pathEncoding = S.uri
        .andFn {
          uri =>
            val base = uri.split(SPLITTER).last
            TestHelper.TEMP_PATH \\ "test-sites" \\ base
        }

      fd.persist()
      val originalVersion = fd.wget(S.uri.andFn(
        {
          uri =>
            val Array(first, last) = uri.split(SPLITTER)
            first + "id_" + SPLITTER + last
        }
      ),
        cooldown = cooldown)
      originalVersion
        .savePages(pathEncoding, overwrite = true)

      fd
    }
  }

  def main(args: Array[String]): Unit = {

    import com.tribbloids.spookystuff.dsl.DSL._


    val spooky = this.spooky

    spooky.wget(
      "https://web.archive.org/web/20170707111752/http://webscraper.io:80/test-sites"
    )
      .save()
      .wgetJoin(S"h2.site-heading a", cooldown = cooldown)
      .save()
      .wgetExplore(S"div.sidebar-nav a", cooldown = cooldown)
      .save()
      .wgetExplore(S"ul.pagination a", cooldown = cooldown)
      .save()
  }
}
