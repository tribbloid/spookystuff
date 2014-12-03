package org.tribbloid.spookystuff.dsl

import scala.util.Random

/**
 * Created by peng on 11/4/14.
 */
case class ProxySetting (
                       addr: String,
                       port: Int,
                       protocol: String
                       ) {

}

object TorProxyFactory extends (() => ProxySetting) {
  def apply() = ProxySetting("127.0.0.1", 9050, "socks5")
}

case class RandomProxyFactory(proxies: Seq[ProxySetting]) extends (() => ProxySetting) {
  def apply() = proxies(Random.nextInt(proxies.size))
}