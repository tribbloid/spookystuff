package com.tribbloids.spookystuff.testutils

/**
  * Created by peng on 17/05/16.
  */
trait LocalURIDocsFixture extends LocalPathDocsFixture {

  override def HTML_URL: String = "file://" + super.HTML_URL
  override def JSON_URL: String = "file://" + super.JSON_URL
  override def PNG_URL: String = "file://" + super.PNG_URL
  override def PDF_URL: String = "file://" + super.PDF_URL
  override def XML_URL: String = "file://" + super.XML_URL
  override def CSV_URL: String = "file://" + super.CSV_URL

  override def DIR_URL: String = "file://" + super.DIR_URL
  override def DEEP_DIR_URL: String = "file://" + super.DEEP_DIR_URL
}
