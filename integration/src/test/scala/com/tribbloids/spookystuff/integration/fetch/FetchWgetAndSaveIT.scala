package com.tribbloids.spookystuff.integration.fetch

import com.tribbloids.spookystuff.actions._
import com.tribbloids.spookystuff.doc.DocUtils
import com.tribbloids.spookystuff.dsl._
import com.tribbloids.spookystuff.extractors.impl.Lit
import com.tribbloids.spookystuff.integration.IntegrationFixture
import com.tribbloids.spookystuff.utils.CommonConst

class FetchWgetAndSaveIT extends IntegrationFixture {

  private val imageURL = {
    "http://upload.wikimedia.org/wikipedia/en/thumb/8/80/Wikipedia-logo-v2.svg/220px-Wikipedia-logo-v2.svg.png"
  }

  override lazy val driverFactories = Seq(
    null
  )

  override def doMain() {

    val fetched = spooky
      .fetch(
        Wget(imageURL)
      )
      .select(Lit("Wikipedia") ~ 'name)
      .persist()
    //    fetched.count()

    val RDD = fetched
      .savePages_!(x"file://${CommonConst.USER_DIR}/temp/spooky-integration/save/${'name}", overwrite = true)
      .select(S.saved ~ 'saved_path)

    val savedPageRows = RDD.unsquashedRDD.collect()

    val finishTime = System.currentTimeMillis()
    assert(savedPageRows.length === 1)
    assert(savedPageRows(0).docs.length === 1)
    val pageTime = savedPageRows(0).docs.head.timeMillis
    assert(pageTime < finishTime)
    assert(pageTime > finishTime-60000) //long enough even after the second time it is retrieved from s3 cache

    val content = savedPageRows(0).docs.head.raw

    assert(
      savedPageRows(0).dataRow.get('saved_path).get.asInstanceOf[Iterable[Any]].toSeq contains
        s"file:${CommonConst.USER_DIR}/temp/spooky-integration/save/Wikipedia.png"
    )

    val loadedContent = DocUtils.load(s"file://${CommonConst.USER_DIR}/temp/spooky-integration/save/Wikipedia.png")(spooky)

    assert(loadedContent === content)

    Thread.sleep(10000) //this delay is necessary to circumvent eventual consistency of HDFS-based cache

    val RDD2 = RDD
      .fetch(
        Wget(imageURL).as('b)
      )

    val unionRDD = RDD.union(RDD2)
    val unionRows = unionRDD.unsquashedRDD.collect()

    assert(unionRows.length === 2)
    assert(
      unionRows(0).docs.head.copy(timeMillis = 0, raw = null, saved = null) ===
        unionRows(1).docs.head.copy(timeMillis = 0, raw = null, saved = null)
    )

    assert(unionRows(0).docs.head.timeMillis === unionRows(1).docs.head.timeMillis)
    assert(unionRows(0).docs.head.raw === unionRows(1).docs.head.raw)
    assert(unionRows(0).docs.head.raw === unionRows(1).docs.head.raw)
    assert(unionRows(1).docs.head.name === "b")
  }

  override def numPages= spooky.spookyConf.defaultGenPartitioner match {
    //    case FetchOptimizers.WebCacheAware => 1
    case _ => 1
  }

  override def pageFetchedCap = 36 // way too large
}