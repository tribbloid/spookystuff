package org.tribbloid.spookystuff.http;

import org.eclipse.jetty.util.URIUtil;

import java.net.URI;
import java.net.URISyntaxException;

/**
 * Created by peng on 2/4/15.
 */
public class HttpUtils {

  public static URI uri(String s) throws URISyntaxException {

    URI uri;
    try {
      uri = new URI(s);
    }
    catch (URISyntaxException e) {
      uri = new URI(URIUtil.encodePath(s));
    }

    return uri.normalize();
  }
}