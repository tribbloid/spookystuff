package org.tribbloid.spookystuff.factory

/**
 * Created by peng on 11/4/14.
 */
case class ProxySetting (
                       addr: String,
                       port: Int,
                       protocol: String
                       ) {

}

object TorProxySetting{
  def apply() = ProxySetting("127.0.0.1", 9050, "socks5")
}