package com.tribbloids.spookystuff.relay.xml

import org.json4s.*

/**
  * Functions to convert between JSON and XML.
  */
//TODO: need testing
object Xml {

  import scala.xml.*

  /**
    * Convert given XML to JSON. <p> Following rules are used in conversion. <ul> <li>XML leaf element is converted to
    * JSON string</li> <li>XML parent element is converted to JSON object and its children to JSON fields</li> <li>XML
    * elements with same name at same level are converted to JSON array</li> <li>XML attributes are converted to JSON
    * fields</li> </ul> <p> Example:<pre> scala> val xml = &lt;users&gt; &lt;user&gt; &lt;id&gt;1&lt;/id&gt;
    * &lt;name&gt;Harry&lt;/name&gt; &lt;/user&gt; &lt;user&gt; &lt;id&gt;2&lt;/id&gt; &lt;name&gt;David&lt;/name&gt;
    * &lt;/user&gt; &lt;/users&gt;
    *
    * scala> val json = toJson(xml) scala> pretty(render(json))
    *
    * { "users":{ "user":[{ "id":"1", "name":"Harry" },{ "id":"2", "name":"David" }] } } </pre>
    *
    * Now, the above example has two problems. First, the id is converted to String while we might want it as an Int.
    * This is easy to fix by mapping JString(s) to JInt(s.toInt). The second problem is more subtle. The conversion
    * function decides to use JSON array because there's more than one user-element in XML. Therefore a structurally
    * equivalent XML document which happens to have just one user-element will generate a JSON document without JSON
    * array. This is rarely a desired outcome. These both problems can be fixed by following map function. <pre> json
    * map { case JField("id", JString(s)) => JField("id", JInt(s.toInt)) case JField("user", x: JObject) =>
    * JField("user", JArray(x :: Nil)) case x => x } </pre>
    */
  def toJson(xml: NodeSeq): JValue = {

    def isEmpty(node: Node) = node.child.isEmpty

    /* Checks if given node is leaf element. For instance these are considered leafs:
     * <foo>bar</foo>, <foo>{ doSomething() }</foo>, etc.
     */
    def isLeaf(node: Node) = {
      def descendant(n: Node): List[Node] = n match {
        case g: Group => g.nodes.toList.flatMap(x => x :: descendant(x))
        case _ =>
          n.child.toList.flatMap { x =>
            x :: descendant(x)
          }
      }

      !descendant(node).exists(_.isInstanceOf[Elem])
    }

    def isArray(nodeNames: Seq[String]) = nodeNames.size != 1 && nodeNames.toList.distinct.size == 1
    def directChildren(n: Node): NodeSeq = n.child.filter(c => c.isInstanceOf[Elem])
    def nameOf(n: Node) = (if (n.prefix ne null) n.prefix + ":" else "") + n.label
    def buildAttrs(n: Node): List[(String, XValue)] = {
      val result = n.attributes.map((a: MetaData) => (a.key, XValue(a.value.text))).toList
      result
    }

    sealed trait XElem
    case class XValue(value: String) extends XElem
    case class XLeaf(value: (String, XElem), attrs: List[(String, XValue)]) extends XElem
    case class XNode(fields: List[(String, XElem)], attrs: List[(String, XValue)]) extends XElem
    case class XArray(elems: List[XElem]) extends XElem

    def toJValue(x: XElem): JValue = x match {
      case XValue(s) => JString(s.trim)
      case XLeaf((name, value), attrs) =>
        (value, attrs) match {
          case (_, Nil)         => toJValue(value)
          case (XValue(""), xs) => JObject(mkAttributes(xs))
          case (_, xs)          => JObject((name, toJValue(value)) :: mkAttributes(xs))
        }
      case XNode(xs, attrs) => JObject(mkFields(xs) ++ mkAttributes(attrs))
      case XArray(elems)    => JArray(elems.map(toJValue))
    }

    def mkAttributes(xs: List[(String, XElem)]) =
      mkFields(xs.map { v =>
        ("@" + v._1) -> v._2
      })

    def mkFields(xs: List[(String, XElem)]) =
      xs.flatMap {
        case (name, value) =>
          (value, toJValue(value)) match {
            // This special case is needed to flatten nested objects which resulted from
            // XML attributes. Flattening keeps transformation more predictable.
            // <a><foo id="1">x</foo></a> -> {"a":{"foo":{"foo":"x","id":"1"}}} vs
            // <a><foo id="1">x</foo></a> -> {"a":{"foo":"x","id":"1"}}
            case (XLeaf(_, _ :: `xs`), o: JObject) => o.obj
            case (_, json)                         => JField(name, json) :: Nil
          }
      }

    def buildNodes(xml: NodeSeq): List[XElem] = xml match {
      case n: Node =>
        if (isEmpty(n)) XLeaf((nameOf(n), XValue("")), buildAttrs(n)) :: Nil
        else if (isLeaf(n)) XLeaf((nameOf(n), XValue(n.text)), buildAttrs(n)) :: Nil
        else {
          val children = directChildren(n)
          XNode(children.map(nameOf).toList.zip(buildNodes(children)), buildAttrs(n)) :: Nil
        }
      case nodes: NodeSeq =>
        val allLabels = nodes.map(_.label)
        if (isArray(allLabels)) {
          val arr = XArray(nodes.toList.flatMap { n =>
            if (isLeaf(n) && n.attributes.length == 0) XValue(n.text) :: Nil
            else buildNodes(n)
          })
          XLeaf((allLabels.head, arr), Nil) :: Nil
        } else nodes.toList.flatMap(buildNodes)
    }

    buildNodes(xml) match {
      case List(x @ XLeaf(_, _ :: _)) => toJValue(x)
      case List(x)                    => JObject(JField(nameOf(xml.head), toJValue(x)) :: Nil)
      case x                          => JArray(x.map(toJValue))
    }
  }

  lazy val ROOT: String = "root"

  /**
    * Convert given JSON to XML. <p> Following rules are used in conversion. <ul> <li>JSON primitives are converted to
    * XML leaf elements</li> <li>JSON objects are converted to XML elements</li> <li>JSON arrays are recursively
    * converted to XML elements</li> </ul> <p> Use <code>map</code> function to preprocess JSON before conversion to
    * adjust the end result. For instance a common conversion is to encode arrays as comma separated Strings since XML
    * does not have array type. <p><pre> toXml(json map { case JField("nums",JArray(ns)) =>
    * JField("nums",JString(ns.map(_.values).mkString(","))) case x => x }) </pre>
    */
  def toXml(json: JValue): NodeSeq = {
    def toXml(name: String, json: JValue): NodeSeq = json match {
      case JObject(fields) =>
        implicit val format: Formats = DefaultFormats
        // We need to build our attributes MetaData object, if we have any.
        var attributes: MetaData = xml.Null

        for (field <- fields) {
          field match {
            case (n, v) if n.startsWith("@") =>
              // If our name (key) starts with "@", remove the "@" and create our attribute
              val name = n.substring(1)
              attributes = new UnprefixedAttribute(name, v.extract[String], attributes)
            case _ => None
          }
        }

        new XmlNode(
          name,
          fields flatMap {
            // Only "toXml" fields that don't start with an "@", since we already handled those as attributes above.
            case (n, v) if !n.startsWith("@") => toXml(n, v)
            case _                            => Text("")
          },
          attributes
        )
      case JArray(xs) =>
        xs flatMap { v =>
          toXml(name, v)
        }
      case JSet(xs) =>
        xs.flatMap { v =>
          toXml(name, v)
        }.toSeq
      case JLong(x)    => new XmlElem(name, x.toString)
      case JInt(x)     => new XmlElem(name, x.toString)
      case JDouble(x)  => new XmlElem(name, x.toString)
      case JDecimal(x) => new XmlElem(name, x.toString)
      case JString(x)  => new XmlElem(name, x)
      case JBool(x)    => new XmlElem(name, x.toString)
      case JNull       => new XmlElem(name, "null")
      case JNothing    => Text("")
    }

    json match {
      case JObject(fields) => fields flatMap { case (n, v) => toXml(n, v) }
      case x               => toXml(ROOT, x)
    }
  }

  class XmlNode(name: String, children: Seq[Node], attributes: MetaData)
      extends Elem(null, name, attributes, TopScope, false, children*)

  class XmlElem(name: String, value: String) extends Elem(null, name, xml.Null, TopScope, false, Text(value))
}
