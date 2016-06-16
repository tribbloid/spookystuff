package com.tribbloids.spookystuff.tests

/**
  * Created by peng on 17/05/16.
  */
trait LocalURIDocsFixture extends LocalPathDocsFixture{

  override def HTML_URL = "file://" + super.HTML_URL
  override def JSON_URL = "file://" + super.JSON_URL
  override def PNG_URL = "file://" + super.PNG_URL
  override def PDF_URL = "file://" + super.PDF_URL
  override def XML_URL = "file://" + super.XML_URL
  override def CSV_URL = "file://" + super.CSV_URL
  override def dirUrl = "file://" + super.dirUrl
}