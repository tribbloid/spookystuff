package com.tribbloids.spookystuff.integration.fetch

import ai.acyclic.prover.commons.spark.Envs
import com.tribbloids.spookystuff.actions._
import com.tribbloids.spookystuff.doc.DocUtils
import com.tribbloids.spookystuff.dsl._
import com.tribbloids.spookystuff.extractors.impl.Lit
import com.tribbloids.spookystuff.integration.ITBaseSpec

class FetchWgetAndSaveIT extends ITBaseSpec {

  private val imageURL = {
    "http://upload.wikimedia.org/wikipedia/en/thumb/8/80/Wikipedia-logo-v2.svg/220px-Wikipedia-logo-v2.svg.png"
  }

  override lazy val webDriverFactories = Seq(
    null
  )

  override def doMain(): Unit = {

    val fetched = spooky
      .fetch(
        Wget(imageURL)
      )
      .extract(Lit("Wikipedia") ~ 'name)
      .persist()
    //    fetched.count()

    val rdd = fetched
      .savePages(x"file://${Envs.USER_DIR}/temp/spooky-integration/save/${'name}", overwrite = true)
      .extract(S.saved ~ 'saved_path)

    val savedPageRows = rdd.fetchedRDD.collect()

    val finishTime = System.currentTimeMillis()
    assert(savedPageRows.length === 1)
    assert(savedPageRows(0).docs.length === 1)
    val pageTime = savedPageRows(0).docs.head.timeMillis
    assert(pageTime < finishTime)
    assert(pageTime > finishTime - 60000) // long enough even after the second time it is retrieved from s3 cache

    val raw = savedPageRows(0).docs.head.blob.raw

    assert(
      savedPageRows(0).dataRow.get('saved_path).get.asInstanceOf[Iterable[Any]].toSeq contains
        s"file:${Envs.USER_DIR}/temp/spooky-integration/save/Wikipedia.png"
    )

    val loaded =
      DocUtils.load(s"file://${Envs.USER_DIR}/temp/spooky-integration/save/Wikipedia.png")(spooky)

    assert(loaded === raw)

    Thread.sleep(10000) // this delay is necessary to circumvent eventual consistency of HDFS-based cache

    val RDD2 = rdd
      .fetch(
        Wget(imageURL).as('b)
      )

    val unionRDD = rdd.union(RDD2)
    val unionRows = unionRDD.fetchedRDD.collect()

    assert(unionRows.length === 2)
    assert(
      unionRows(0).docs.head.copy(timeMillis = 0)(null) ===
        unionRows(1).docs.head.copy(timeMillis = 0)(null)
    )

    unionRows.map(_.docs.head.timeMillis.toString).shouldBeIdentical()
    unionRows.map(_.docs.head.content.contentStr).shouldBeIdentical()

    assert(unionRows(1).docs.head.name === "b")
  }

  override def numPages: Long = spooky.conf.localityPartitioner match {
    //    case FetchOptimizers.WebCacheAware => 1
    case _ => 1
  }

  override def pageFetchedCap: Long = 36 // way too large
}
