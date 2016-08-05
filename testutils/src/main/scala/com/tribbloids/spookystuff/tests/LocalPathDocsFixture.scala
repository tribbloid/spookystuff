package com.tribbloids.spookystuff.tests

/**
  * Created by peng on 17/05/16.
  */
trait LocalPathDocsFixture extends RemoteDocsFixture{

  override def HTML_URL = this.getClass.getClassLoader.getResource("site/Wikipedia.html").getPath
  override def JSON_URL = this.getClass.getClassLoader.getResource("site/tribbloid.json").getPath
  override def PNG_URL = this.getClass.getClassLoader.getResource("site/logo11w.png").getPath
  override def PDF_URL = this.getClass.getClassLoader.getResource("site/Test.pdf").getPath
  override def XML_URL = this.getClass.getClassLoader.getResource("site/pom.xml").getPath
  override def CSV_URL = this.getClass.getClassLoader.getResource("site/table.csv").getPath

  def dirUrl = this.getClass.getClassLoader.getResource("site").getPath
}