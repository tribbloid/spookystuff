package com.tribbloids.spookystuff.pages

import org.apache.commons.csv.{CSVFormat, CSVParser}

object CSVElement {

  def apply(content: Array[Byte], charSet: String, uri: String, cSVFormat: CSVFormat): CSVElement = new CSVElement(
    new String(content, charSet),
    uri,
    cSVFormat
  )
}

/**
  * equivalent to the following xml:
  * <splitter>
  *   <row>
  *     <header1>datum1</header1>
  *     <header2>datum2</header2>
  *     ...
  *   </row>
  *   <row>
  *     <header2>datum1</header2>
  *     <header2>datum1</header2>
  *     ...
  *   </row>
  *   ...
  * </splitter>
  */
class CSVElement(
                  val _text: String,
                  override val uri: String,
                  val csvFormat: CSVFormat
                ) extends Unstructured {

  import scala.collection.JavaConverters._

  val parsed: CSVParser = CSVParser.parse(_text, csvFormat)
  val parsedList = parsed.asScala.toList
  val headers = parsed.getHeaderMap.asScala.keys.toList

  override def text: Option[String] = Some(_text)

  override def breadcrumb: Option[Seq[String]] = ???

  override def children(selector: Selector): Elements[Unstructured] = {
    if (!this.headers.contains(selector)) new EmptyElements
    else {
      val data = parsedList.map {
        record =>
          val datum = record.get(selector)

          new CsvDatum(uri, datum, selector)
      }
      new Elements(
        data
      )
    }
  }

  override def childrenWithSiblings(selector: Selector, range: Range): Elements[Siblings[Unstructured]] = {
    if (!this.headers.contains(selector)) new EmptyElements
    else {
      val data = parsedList.map {
        record =>
          val index = headers.indexOf(selector)
          val siblingHeaders = headers.slice(index + range.min, index + range.max)
          val delimiter = csvFormat.getDelimiter.toString
          new Siblings(
            siblingHeaders.map {
              h =>
                val datum = record.get(h)
                new CsvDatum(uri, datum, selector)
            },
            delimiter,
            delimiter
          )
      }
      new Elements(
        data
      )
    }
  }

  override def ownText: Option[String] = None

  override def src: Option[String] = ownText

  override def formattedCode: Option[String] = text

  override def boilerPipe: Option[String] = text

  override def findAll(selector: Selector): Elements[Unstructured] = children(selector)

  override def findAllWithSiblings(selector: Selector, range: Range): Elements[Siblings[Unstructured]] =
    childrenWithSiblings(selector, range)

  override def href: Option[String] = ownText

  override def code: Option[String] = text

  override def allAttr: Option[Map[String, String]] = Some(Map())

  override def attr(attr: String, noEmpty: Boolean): Option[String] = ownText
}

class CsvDatum(
                override val uri: String,
                val _ownText: String,
                val header: String
              ) extends Unstructured {

  override def findAll(selector: Selector): Elements[Unstructured] = new EmptyElements

  override def text: Option[String] = ownText

  override def breadcrumb: Option[Seq[String]] = ???

  override def children(selector: Selector): Elements[Unstructured] = new EmptyElements

  override def ownText: Option[String] = Some(_ownText)

  override def src: Option[String] = ownText

  override def formattedCode: Option[String] = ownText

  override def boilerPipe: Option[String] = ownText

  override def findAllWithSiblings(selector: Selector, range: Range): Elements[Siblings[Unstructured]] = new EmptyElements

  override def href: Option[String] = ownText

  override def code: Option[String] = ownText

  override def childrenWithSiblings(selector: Selector, range: Range): Elements[Siblings[Unstructured]] = new EmptyElements

  override def allAttr: Option[Map[String, String]] = Some(Map())

  override def attr(attr: String, noEmpty: Boolean): Option[String] = ownText
}