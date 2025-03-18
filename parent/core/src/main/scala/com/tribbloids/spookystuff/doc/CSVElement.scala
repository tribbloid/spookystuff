package com.tribbloids.spookystuff.doc

import org.apache.commons.csv.{CSVFormat, CSVParser}
import org.apache.commons.csv.CSVRecord

import java.io.Reader
import scala.io.Source
import scala.util.Try

trait CSVElement

object CSVElement {

  /**
    * equivalent to the following xml: <splitter> <row> <header1>datum1</header1> <header2>datum2</header2> ... </row>
    * <row> <header2>datum1</header2> <header2>datum1</header2> ... </row> ... </splitter>
    */
  case class Block(
      reader: Reader,
      override val uri: String,
      csvFormat: CSVFormat
  ) extends Unstructured {

    import scala.jdk.CollectionConverters.*

    val parsed: CSVParser = CSVParser.parse(reader, csvFormat)
    val records: List[CSVRecord] = {

      val tt = Try {
        parsed.asScala.toList
      }
      tt.recover { (e: Exception) =>
        throw new RuntimeException(s"Error reading ${uri}\n(using format ${csvFormat})\n  ${e.getMessage()}", e)
      }.get
    }
    val headers: List[String] = parsed.getHeaderMap.asScala.keys.toList

    @transient override lazy val text: Option[String] = {

      Some(parsed.toString())
    }

    override def breadcrumb: Option[Seq[String]] = None

    override def children(selector: DocQuery): Elements[Unstructured] = {
      val _selector = selector.toString

      if (!this.headers.contains(_selector)) Elements.empty
      else {
        val data = records.map { record =>
          val datum = record.get(_selector)

          new Cell(uri, datum, _selector)
        }
        Elements(
          data
        )
      }
    }

    override def childrenWithSiblings(selector: DocQuery, range: Range): Elements[Siblings[Unstructured]] = {
      if (!this.headers.contains(selector)) Elements.empty
      else {
        val data = records.map { record =>
          val index = headers.indexOf(selector.toString)
          val siblingHeaders = headers.slice(index + range.min, index + range.max)
          val delimiter = csvFormat.getDelimiter.toString
          new Siblings(
            siblingHeaders.map { h =>
              val datum = record.get(h)
              new Cell(uri, datum, selector.toString)
            },
            delimiter,
            delimiter
          )
        }
        Elements(
          data
        )
      }
    }

    override def ownText: Option[String] = None

    override def src: Option[String] = ownText

    override def formattedCode: Option[String] = text

    override def boilerPipe: Option[String] = text

    override def findAll(selector: DocQuery): Elements[Unstructured] = children(selector)

    override def findAllWithSiblings(selector: DocQuery, range: Range): Elements[Siblings[Unstructured]] =
      childrenWithSiblings(selector, range)

    override def href: Option[String] = ownText

    override def code: Option[String] = text

    override def allAttr: Option[Map[String, String]] = Some(Map())

    override def attr(attr: String, noEmpty: Boolean): Option[String] = ownText
  }

  class Cell(
      override val uri: String,
      val _ownText: String,
      val header: String
  ) extends Unstructured {

    override def findAll(selector: DocQuery): Elements[Unstructured] = Elements.empty

    override def text: Option[String] = ownText

    override def breadcrumb: Option[Seq[String]] = ???

    override def children(selector: DocQuery): Elements[Unstructured] = Elements.empty

    override def ownText: Option[String] = Some(_ownText)

    override def src: Option[String] = ownText

    override def formattedCode: Option[String] = ownText

    override def boilerPipe: Option[String] = ownText

    override def findAllWithSiblings(selector: DocQuery, range: Range): Elements[Siblings[Unstructured]] =
      Elements.empty

    override def href: Option[String] = ownText

    override def code: Option[String] = ownText

    override def childrenWithSiblings(selector: DocQuery, range: Range): Elements[Siblings[Unstructured]] =
      Elements.empty

    override def allAttr: Option[Map[String, String]] = Some(Map())

    override def attr(attr: String, noEmpty: Boolean): Option[String] = ownText
  }
}
