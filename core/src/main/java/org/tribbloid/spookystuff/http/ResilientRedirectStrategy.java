package org.tribbloid.spookystuff.http;

import org.apache.http.ProtocolException;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.impl.client.LaxRedirectStrategy;
import org.apache.http.util.TextUtils;

import java.net.*;
import java.util.Locale;

/**
 * Created by peng on 2/4/15.
 */
public class ResilientRedirectStrategy extends LaxRedirectStrategy {

//  @Override
  protected URI createLocationURI(final String location) throws ProtocolException {
    // One can try to rewrite malformed redirect locations

    try {
      URI uri = HttpUtils.uri(location);
      final URIBuilder b = new URIBuilder(uri);
      final String host = b.getHost();
      if (host != null) {
        b.setHost(host.toLowerCase(Locale.ENGLISH));
      }
      final String path = b.getPath();
      if (TextUtils.isEmpty(path)) {
        b.setPath("/");
      }
      return b.build();
    } catch (final URISyntaxException ex) {
      throw new ProtocolException("Invalid redirect URI: " + location, ex);
    }

    //at this point
//    try {
//      return HttpUtils.uri(location);
//    } catch (MalformedURLException | URISyntaxException e) {
//      throw new ProtocolException("malformed location", e);
//    }
  }
}