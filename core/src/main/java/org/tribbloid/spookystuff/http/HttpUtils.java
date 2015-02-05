package org.tribbloid.spookystuff.http;

import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;

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
//      URL url = new URL(s);
//      url.to
//
//      uri = new URI(url.getProtocol(), url.getUserInfo(), url.getHost(), url.getPort(), url.getPath(), url.getQuery(), url.getRef());
      throw e;
    }

    return uri.normalize();
  }
}