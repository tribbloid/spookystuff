package com.tribbloids.spookystuff.integration.explore

import com.tribbloids.spookystuff.actions.Wget
import com.tribbloids.spookystuff.integration.IntegrationFixture
import com.tribbloids.spookystuff.testutils.LocalPathResolver

/**
  * Created by peng on 29/01/17.
  */
class ExploreDirectories extends IntegrationFixture {

  import com.tribbloids.spookystuff.dsl._

  override lazy val driverFactories = Seq(
    null
  )

  val resourcePath = LocalPathResolver.unpackedURL("testutils/dir").getPath

  override def doMain(): Unit = {
    val url = resourcePath
    val rootDoc = spooky.create(Seq(url))
      .fetch{
        Wget('_)
      }
    val nodes = rootDoc
      .explore(S"root directory".attr("path"))(
        Wget('A)
      )()
      .extract(S.uri ~ 'uri)
    val result = nodes
      .flatExtract(S"root file")(
        'A.ownText ~ 'leaf,
        'A.attr("path") ~ 'fullPath
      )
      .toDF(sort = true)

    result.schema.treeString.shouldBe(
      """
        |root
        | |-- _: string (nullable = true)
        | |-- uri: string (nullable = true)
        | |-- leaf: string (nullable = true)
        | |-- fullPath: string (nullable = true)
      """.stripMargin
    )

    val formatted = result.toJSON.collect().mkString("\n")
    formatted.shouldBe (
      s"""
         |{"_":"$resourcePath","uri":"$resourcePath","leaf":"table.csv","fullPath":"file:$resourcePath/table.csv"}
         |{"_":"$resourcePath","uri":"$resourcePath","leaf":"hivetable.csv","fullPath":"file:$resourcePath/hivetable.csv"}
         |{"_":"$resourcePath","uri":"file:$resourcePath/dir","leaf":"Test.pdf","fullPath":"file:$resourcePath/dir/Test.pdf"}
         |{"_":"$resourcePath","uri":"file:$resourcePath/dir/dir","leaf":"pom.xml","fullPath":"file:$resourcePath/dir/dir/pom.xml"}
         |{"_":"$resourcePath","uri":"file:$resourcePath/dir/dir/dir","leaf":"tribbloid.json","fullPath":"file:$resourcePath/dir/dir/dir/tribbloid.json"}
      """.stripMargin
    )
  }

  override def numPages: Int = 4
  override def pageFetchedCap: Int = 16 // way too large
}
