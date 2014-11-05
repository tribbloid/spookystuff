package org.tribbloid.spookystuff.actions

/**
 * Created by peng on 11/4/14.
 */
abstract class ProxySetting (
                       val addr: String,
                       val port: Int,
                       val protocol: String
                       ) {

}

object TorProxySetting extends ProxySetting("127.0.0.1", 9050, "socks5")